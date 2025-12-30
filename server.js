/**
 * ═══════════════════════════════════════════════════════════════════════════
 * 🔥 ملك الطابون - Backend المركزي (العقل) - النسخة المطورة - Node.js
 * ═══════════════════════════════════════════════════════════════════════════
 * 
 * هذا هو الوسيط الآمن بين موقع العملاء وموقع العمال
 * 
 * المسؤوليات:
 * 1. حماية مفتاح OpenAI API
 * 2. التواصل مع الذكاء الاصطناعي
 * 3. استقبال وتخزين الطلبات
 * 4. توزيع البيانات للموقعين
 * 5. Rate Limiting للحماية من الإساءة
 * 6. مسح تلقائي للطلبات القديمة
 * 7. ✅ دعم ثلاثة أنواع من الزبائن
 * 8. ✅ WebSocket للإشعارات الفورية للعملاء
 * 
 * ═══════════════════════════════════════════════════════════════════════════
 */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const { WebSocketServer } = require('ws');
const OpenAI = require('openai');
const http = require('http');

// ═══════════════════════════════════════════════════════════════════════════
// 🔐 الإعدادات الحساسة
// ═══════════════════════════════════════════════════════════════════════════


if (!OPENAI_API_KEY) {
    console.log('⚠️  تحذير: مفتاح OpenAI غير مُعد! أضفه في متغيرات البيئة');
}

// إنشاء OpenAI client
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

// ═══════════════════════════════════════════════════════════════════════════
// 🚀 إنشاء التطبيق
// ═══════════════════════════════════════════════════════════════════════════

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(cors({
    origin: '*',
    credentials: true,
    methods: ['GET', 'POST', 'PATCH', 'DELETE'],
}));
app.use(helmet({
    contentSecurityPolicy: false,
}));

// Rate Limiters
const chatLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 دقيقة
    max: 20,
    message: { success: false, error: 'تم تجاوز الحد المسموح من الطلبات' }
});

const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 60,
    message: { success: false, error: 'تم تجاوز الحد المسموح من الطلبات' }
});

// ═══════════════════════════════════════════════════════════════════════════
// 📦 قاعدة البيانات المؤقتة
// ═══════════════════════════════════════════════════════════════════════════

const db = {
    orders: [],
    counter: 1000,
    lastCleanup: new Date().toISOString().split('T')[0],
    lastKnownId: 1000
};

// ═══════════════════════════════════════════════════════════════════════════
// 🔌 WebSocket Connection Manager - إدارة اتصالات العملاء
// ═══════════════════════════════════════════════════════════════════════════

const wss = new WebSocketServer({ server, path: '/ws/notifications' });

class ConnectionManager {
    constructor() {
        this.activeConnections = new Map(); // orderId -> [clients]
        this.allConnections = [];
    }

    connect(ws, orderId = null) {
        this.allConnections.push(ws);
        
        if (orderId) {
            if (!this.activeConnections.has(orderId)) {
                this.activeConnections.set(orderId, []);
            }
            this.activeConnections.get(orderId).push(ws);
            console.log(`🔗 عميل متصل لمتابعة الطلب #${orderId}`);
        } else {
            console.log('🔗 عميل متصل (بدون طلب محدد)');
        }
    }

    disconnect(ws, orderId = null) {
        const index = this.allConnections.indexOf(ws);
        if (index > -1) {
            this.allConnections.splice(index, 1);
        }

        if (orderId && this.activeConnections.has(orderId)) {
            const connections = this.activeConnections.get(orderId);
            const idx = connections.indexOf(ws);
            if (idx > -1) {
                connections.splice(idx, 1);
            }
            if (connections.length === 0) {
                this.activeConnections.delete(orderId);
            }
        }
        console.log('🔌 عميل قطع الاتصال');
    }

    sendToOrder(orderId, message) {
        if (this.activeConnections.has(orderId)) {
            const connections = this.activeConnections.get(orderId);
            connections.forEach(ws => {
                if (ws.readyState === 1) { // OPEN
                    try {
                        ws.send(JSON.stringify(message));
                        console.log(`📤 تم إرسال إشعار للطلب #${orderId}`);
                    } catch (e) {
                        console.log(`⚠️ خطأ في الإرسال: ${e.message}`);
                    }
                }
            });
        }
    }

    broadcast(message) {
        const disconnected = [];
        this.allConnections.forEach(ws => {
            if (ws.readyState === 1) {
                try {
                    ws.send(JSON.stringify(message));
                } catch (e) {
                    disconnected.push(ws);
                }
            } else {
                disconnected.push(ws);
            }
        });

        // إزالة الاتصالات المنقطعة
        disconnected.forEach(ws => {
            const idx = this.allConnections.indexOf(ws);
            if (idx > -1) {
                this.allConnections.splice(idx, 1);
            }
        });

        if (message.type === 'order_ready') {
            console.log(`📢 تم بث إشعار جاهزية للجميع - الطلب #${message.orderId}`);
        }
    }
}

const manager = new ConnectionManager();

// WebSocket Server
wss.on('connection', (ws, req) => {
    // تحديد إذا كان الاتصال لطلب محدد
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const orderId = urlParams.get('orderId');
    
    manager.connect(ws, orderId ? parseInt(orderId) : null);

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'subscribe' && msg.orderId) {
                const orderId = parseInt(msg.orderId);
                if (!manager.activeConnections.has(orderId)) {
                    manager.activeConnections.set(orderId, []);
                }
                manager.activeConnections.get(orderId).push(ws);
                console.log(`📌 العميل اشترك لمتابعة الطلب #${orderId}`);
            }
        } catch (e) {
            // ignore
        }
    });

    ws.on('close', () => {
        manager.disconnect(ws, orderId ? parseInt(orderId) : null);
    });
});

// ═══════════════════════════════════════════════════════════════════════════
// 🧹 تنظيف تلقائي
// ═══════════════════════════════════════════════════════════════════════════

function autoCleanup() {
    const today = new Date().toISOString().split('T')[0];
    
    if (today !== db.lastCleanup) {
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        yesterday.setHours(0, 0, 0, 0);
        
        const before = db.orders.length;
        db.orders = db.orders.filter(o => new Date(o.createdAt) >= yesterday);
        
        if (before !== db.orders.length) {
            console.log(`🧹 تنظيف تلقائي: حذف ${before - db.orders.length} طلب قديم`);
        }
        
        db.lastCleanup = today;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 🕐 مسح يومي الساعة 5 فجراً بتوقيت القدس
// ═══════════════════════════════════════════════════════════════════════════

let lastCleanupDate = '';

async function dailyCleanupAt5am() {
    setInterval(() => {
        try {
            // الحصول على الوقت بتوقيت القدس (UTC+2 أو UTC+3)
            const now = new Date();
            const jerusalemOffset = 2 * 60; // UTC+2 (قد تحتاج تعديل للتوقيت الصيفي)
            const jerusalemTime = new Date(now.getTime() + (jerusalemOffset + now.getTimezoneOffset()) * 60000);
            
            const hours = jerusalemTime.getHours();
            const minutes = jerusalemTime.getMinutes();
            const todayDate = jerusalemTime.toISOString().split('T')[0];
            
            // إذا كانت الساعة 5:00-5:01 فجراً ولم يتم المسح اليوم
            if (hours === 5 && minutes < 2 && lastCleanupDate !== todayDate) {
                const deletedCount = db.orders.length;
                db.orders = [];
                db.counter = 1000;
                lastCleanupDate = todayDate;
                
                console.log('\n╔════════════════════════════════════════════════════════════╗');
                console.log('║  🧹 مسح يومي - الساعة 5:00 فجراً بتوقيت القدس              ║');
                console.log(`║  📊 تم حذف جميع الطلبات: ${deletedCount} طلب                         ║`);
                console.log('║  ✅ النظام جاهز ليوم جديد                                  ║');
                console.log('╚════════════════════════════════════════════════════════════╝\n');
            }
        } catch (e) {
            console.log(`خطأ في المسح اليومي: ${e.message}`);
        }
    }, 30000); // فحص كل 30 ثانية
}

// ═══════════════════════════════════════════════════════════════════════════
// 🤖 System Prompt للذكاء الاصطناعي
// ═══════════════════════════════════════════════════════════════════════════

const SYSTEM_PROMPT = `# وكيل طلبات مطعم ملك الطابون

## هويتك
أنت مساعد طلبات مطعم "ملك الطابون والمعجنات" في العيزرية. تستقبل طلبات الزبائن باللهجة الفلسطينية.

## معلومات المطعم
- الاسم: ملك الطابون
- العنوان: العيزرية - دوار وادي النار
- ساعات العمل: 8:00 صباحاً - 2:00 بعد منتصف الليل (يومياً)
- وقت التحضير: 7-10 دقيقة الوجبة دون وقت التوصيل
- التوصيل: متوفر للعيزرية والمناطق المجاورة
- الدفع: كاش أو فيزا داخل المحل فقط
- التوصيل لأماكن في العيزرية ب شيكل 15 والسواحرة ب 20 شيكل

## قواعد السلوك
1. تحدث بلهجة فلسطينية بسيطة وودودة
2. كن مختصراً - لا تكتب رسائل طويلة
3. لا تعرض القائمة كاملة - اعرض فقط خيارات الصنف المطلوب
4. استخدم إيموجي واحد أو اثنين فقط
5. لا تخترع أصناف غير موجودة ولا تغير الأسعار
6. فقط لمن هو بالمحل او بالسيارة أطلب منه الاسم + الطلب.
7. قم بإخبار الزبون بالسعر مع طلب التأكيد
8. بمجرد التأكيد, أطلب منه إستلام الطلب بعد 7-10 دقائق, طلبات التوصيل تستغرق 10-20 دقيقة حسب الموقع والطلب
9. في حال طلب  "معجنات مناسبات" أخبره أن يتواصل معنا عبر الواتساب 0523668131

## خاص لطلبيات التوصيل
- اطلب منه رقم الجوال (إجباري) ولا حاجة للإسم. 
- اخذ عنوان الموقع واسم البيت او مكان العمل. 
- اخبارهم ان التوصيل يكلف  10-20 شيكل حسب الموقع بأماكن العيزرية أو السواحرة.

## ✅ نظام أنواع الزبائن الثلاثة - مهم جداً!

### المعلومات المطلوبة حسب نوع الزبون:

**1️⃣ زبون داخل المحل (dine_in):**
- الاسم
- الطلب
- فقط! لا حاجة لمزيد من التفاصيل

**2️⃣ زبون بالسيارة (car_pickup):**
- الاسم
- الطلب
- رقم الجوال (للتواصل لما يكون الطلب جاهز)
- لون السيارة أو نوعها (اختياري لتسهيل التعرف)

**3️⃣ زبون توصيل (delivery):**
- الاسم
- الطلب
- رقم الجوال (إجباري)
- العنوان بالتفصيل (الحي/الشارع/أقرب نقطة معروفة)
- ملاحظات التوصيل (اختياري)

## تنسيق الطلب النهائي - مهم جداً!
عندما يؤكد الزبون طلبه، يجب أن تضيف في نهاية ردك هذا التنسيق بالضبط:

[ORDER_DATA]
{
  "customer": "اسم الزبون",
  "phone": "رقم الجوال أو فارغ",
  "items": "الأصناف المطلوبة",
  "total": المبلغ_رقم,
  "orderType": "dine_in أو car_pickup أو delivery",
  "location": "داخل المحل أو بالسيارة أو اسم المنطقة للتوصيل",
  "address": "العنوان التفصيلي للتوصيل فقط",
  "carInfo": "معلومات السيارة للزبون بالسيارة",
  "deliveryNotes": "ملاحظات التوصيل"
}
[/ORDER_DATA]

## القائمة الكاملة

### البيتزا أو معجنات الطابون
- بيتزا الطابون: 20 شيكل
- بيتزا بالجبنة البيضاء: 23 شيكل
- بيتزا تونا: 25 شيكل
- بيتزا ستيك دجاج: 30 شيكل
- بيتزا سلامي: 25 شيكل
- بيتزا مكس جبنة: 22 شيكل
- بيتزا مكسيكي حار: 30 شيكل
- بيتزا نقانق: 23 شيكل
- بيتزا عيمك خضار: 25 شيكل
- بيتزا عيمك ستيك: 35 شيكل
- بيتزا عيمك سلامي: 30 شيكل
- بيتزا عيمك مكسيكي: 35 شيكل
- بيتزا عيمك نقانق: 28 شيكل

### البيض
- بيض سادة: 8 شيكل
- بيض مع جبنة بيضاء: 15 شيكل
- بيض مع جبنة عيمك: 21 شيكل
- بيض مع جبنة موزاريلا: 14 شيكل
- بيض مع جبنة ونقانق: 17 شيكل
- بيض مع زيتون ودرة: 12 شيكل
- بيض مع سجق: 20 شيكل
- بيض مع سجق وجبنة: 22 شيكل
- بيض مع سجق وجبنة عيمك: 28 شيكل
- بيض مع سجق وجبنة ونقانق: 28 شيكل
- بيض مع عيمك ونقانق: 23 شيكل
- بيض مع لحمة بالجبنة: 25 شيكل
- بيض مع لحمة طازجة: 20 شيكل
- بيض مع نقانق: 13 شيكل

### الجبنة
- جبنة بيضاء مع بندورة: 18 شيكل
- جبنة بيضاء مع حبة البركة: 15 شيكل
- جبنة بيضاء مع زعتر أخضر: 17 شيكل
- جبنة بيضاء مع زيت وزعتر: 17 شيكل
- جبنة بيضاء مع زيتون أخضر: 17 شيكل
- جبنة عيمك: 23 شيكل
- جبنة عيمك مع ستيك دجاج: 30 شيكل
- جبنة عيمك مع سلامي: 28 شيكل
- جبنة عيمك مع نقانق: 27 شيكل
- جبنة موزاريلا: 18 شيكل
- جبنة موزاريلا بالدجاج المكسيكي الحار: 28 شيكل
- جبنة موزاريلا مع زيتون أخضر: 20 شيكل
- جبنة موزاريلا مع ستيك الدجاج: 28 شيكل
- جبنة موزاريلا مع سلامي: 25 شيكل
- جبنة موزاريلا مع نقانق: 23 شيكل

### اللحمة والسفيحة
- سفيحة بالبندورة: 15 شيكل
- سفيحة بالطحينية: 15 شيكل
- سفيحة مكس: 15 شيكل
- فاهيتا جبنة: 23 شيكل
- فاهيتا دجاج: 23 شيكل
- كبدة دجاج: 20 شيكل
- لحمة طازجة: 25 شيكل
- مسحب مع جبنة: 25 شيكل
- مسخن دجاج: 22 شيكل

### الصواني
- صواني كبير (شوي): 30 شيكل
- صواني كبير مع خضار: 40 شيكل
- صواني وسط (شوي): 20 شيكل
- صواني وسط مع خضار: 30 شيكل

### الخبز والمناقيش
- خبز الطابون: 1.50 شيكل
- خبز شراك: 1 شيكل
- مناقيش زعتر: 7 شيكل
- 7 طابون: 10 شيكل

### المعجنات الصغيرة
- جبنة بيضاء صغير: 5 شيكل
- جبنة بيضاء مع زعتر صغير: 7 شيكل
- جبنة مع زيتون أخضر صغير: 6 شيكل
- جبنة مع نقانق صغير: 7 شيكل
- ريانة لحمة: 8 شيكل
- سبانخ: 6 شيكل
- عرايس: 8 شيكل
- لفة مسخن: 8 شيكل
- مسحب دجاج: 10 شيكل

### المقبلات
- شيلي متوك: 3 شيكل
- صوص فهيم: 3 شيكل
- علبة باربكيو: 3 شيكل
- علبة زيتون: 2 شيكل
- علبة لفت صغير: 2 شيكل
- علبة مخلل صغير: 2 شيكل
- فلفل مكبوس صغير: 2 شيكل
- نص طبق بيض: 10 شيكل

### المشروبات
- ماء صغير: 2 شيكل
- ماء كبير: 3 شيكل
- كابي صغير: 3 شيكل
- لبن أب: 3 شيكل
- عصير تبوزينا: 4 شيكل
- صودا: 5 شيكل
- مشروب غازي 330: 3 شيكل
- مشروب غازي 1.25: 5 شيكل
- BLUE: 5 شيكل
- TASCO: 5 شيكل
- XL: 5 شيكل
- بافاريا: 5 شيكل

## الردود الخاصة
- التوصيل: "متوفر للعيزرية والمناطق المجاورة"
- الدفع: "كاش أو فيزا داخل المحل"
- ساعات العمل: "من 8 الصبح لـ 2 بالليل، كل يوم"
- صنف غير موجود: "للأسف مش متوفر، بقترح عليك [بديل]"
- في حال طلب "معجنات مناسبات" أخبره أن يتواصل معنا عبر الواتساب 0523668131`;

// ═══════════════════════════════════════════════════════════════════════════
// 🔌 API Endpoints
// ═══════════════════════════════════════════════════════════════════════════

// محادثة مع الذكاء الاصطناعي
app.post('/api/chat', chatLimiter, async (req, res) => {
    try {
        const { message, history = [] } = req.body;

        if (!message) {
            return res.status(400).json({ success: false, error: 'الرسالة مطلوبة' });
        }

        // بناء المحادثة
        const messages = [{ role: 'system', content: SYSTEM_PROMPT }];

        // إضافة التاريخ (آخر 10 رسائل)
        if (history && history.length > 0) {
            history.slice(-10).forEach(msg => {
                messages.push({
                    role: msg.role === 'user' ? 'user' : 'assistant',
                    content: msg.content
                });
            });
        }

        messages.push({ role: 'user', content: message });

        // استدعاء OpenAI API
        const completion = await openai.chat.completions.create({
            model: 'gpt-4o-mini',
            messages: messages,
            max_tokens: 500,
            temperature: 0.7
        });

        let reply = completion.choices[0].message.content;

        // استخراج بيانات الطلب
        const orderMatch = reply.match(/\[ORDER_DATA\]([\s\S]*?)\[\/ORDER_DATA\]/);
        let orderId = null;

        if (orderMatch) {
            try {
                const orderData = JSON.parse(orderMatch[1].trim());

                // إنشاء الطلب
                db.counter += 1;
                const order = {
                    id: db.counter,
                    customerName: orderData.customer || 'عميل',
                    phone: orderData.phone || '',
                    items: orderData.items || '',
                    total: parseFloat(orderData.total || 0),
                    orderType: orderData.orderType || 'dine_in',
                    location: orderData.location || 'غير محدد',
                    address: orderData.address || '',
                    carInfo: orderData.carInfo || '',
                    deliveryNotes: orderData.deliveryNotes || '',
                    status: 'new',
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString(),
                    source: 'AI_Chat'
                };

                db.orders.unshift(order);
                orderId = order.id;

                // عرض معلومات الطلب
                const orderTypeLabels = {
                    'dine_in': '🏠 داخل المحل',
                    'car_pickup': '🚗 بالسيارة',
                    'delivery': '🛵 توصيل'
                };

                console.log(`\n${'═'.repeat(50)}`);
                console.log(`🔔 طلب جديد #${order.id} - ${orderTypeLabels[order.orderType] || 'غير محدد'}`);
                console.log('═'.repeat(50));
                console.log(`   👤 الاسم: ${order.customerName}`);
                if (order.phone) {
                    console.log(`   📱 الجوال: ${order.phone}`);
                }
                console.log(`   🍕 الطلب: ${order.items}`);
                console.log(`   💰 المبلغ: ${order.total} شيكل`);

                if (order.orderType === 'car_pickup' && order.carInfo) {
                    console.log(`   🚗 السيارة: ${order.carInfo}`);
                }

                if (order.orderType === 'delivery') {
                    console.log(`   📍 الموقع: ${order.location}`);
                    console.log(`   🏠 العنوان: ${order.address}`);
                    if (order.deliveryNotes) {
                        console.log(`   📝 ملاحظات: ${order.deliveryNotes}`);
                    }
                }
                console.log('═'.repeat(50) + '\n');

                // إزالة بيانات الطلب من الرد
                reply = reply.replace(/\[ORDER_DATA\][\s\S]*?\[\/ORDER_DATA\]/, '').trim();
                reply += `\n\n📋 رقم طلبك: #${orderId}`;

            } catch (e) {
                console.log(`Error parsing order: ${e.message}`);
            }
        }

        res.json({
            success: true,
            reply: reply,
            orderId: orderId
        });

    } catch (error) {
        console.log(`Chat Error: ${error.message}`);
        res.status(500).json({
            success: false,
            error: 'حدث خطأ في الخدمة',
            reply: 'عذراً، حصل خطأ. حاول مرة أخرى'
        });
    }
});

// جلب جميع الطلبات
app.get('/api/orders', apiLimiter, (req, res) => {
    autoCleanup();

    const { orderType } = req.query;
    let filteredOrders = db.orders;

    if (orderType && ['dine_in', 'car_pickup', 'delivery'].includes(orderType)) {
        filteredOrders = db.orders.filter(o => o.orderType === orderType);
    }

    res.json({
        success: true,
        orders: filteredOrders,
        total: filteredOrders.length,
        byType: {
            dine_in: db.orders.filter(o => o.orderType === 'dine_in').length,
            car_pickup: db.orders.filter(o => o.orderType === 'car_pickup').length,
            delivery: db.orders.filter(o => o.orderType === 'delivery').length
        }
    });
});

// إنشاء طلب يدوي
app.post('/api/orders', apiLimiter, (req, res) => {
    const { customerName, phone, items, total, orderType, location, address, carInfo, deliveryNotes, notes } = req.body;

    if (!customerName || !items) {
        return res.status(400).json({ success: false, error: 'اسم العميل والطلب مطلوبان' });
    }

    db.counter += 1;
    const order = {
        id: db.counter,
        customerName,
        phone: phone || '',
        items,
        total: total || 0,
        orderType: orderType || 'dine_in',
        location: location || 'داخل المحل',
        address: address || '',
        carInfo: carInfo || '',
        deliveryNotes: deliveryNotes || '',
        notes: notes || '',
        status: 'new',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        source: 'Manual'
    };

    db.orders.unshift(order);
    console.log(`📝 طلب يدوي #${order.id}: ${order.customerName} - ${order.orderType}`);

    res.json({ success: true, order });
});

// جلب طلب واحد
app.get('/api/orders/:orderId', (req, res) => {
    const orderId = parseInt(req.params.orderId);
    const order = db.orders.find(o => o.id === orderId);

    if (!order) {
        return res.status(404).json({ success: false, error: 'الطلب غير موجود' });
    }

    const statusText = {
        'new': 'تم استلام طلبك',
        'preparing': 'جاري تحضير طلبك',
        'ready': 'طلبك جاهز للاستلام! 🎉',
        'delivered': 'تم تسليم الطلب',
        'cancelled': 'تم إلغاء الطلب'
    };

    res.json({
        success: true,
        id: order.id,
        status: order.status,
        items: order.items,
        total: order.total,
        orderType: order.orderType,
        statusText: statusText[order.status],
        notification: order.readyNotification,
        updatedAt: order.updatedAt
    });
});

// تحديث حالة الطلب - مع إرسال WebSocket
app.patch('/api/orders/:orderId', async (req, res) => {
    const orderId = parseInt(req.params.orderId);
    const { status, notes } = req.body;

    const order = db.orders.find(o => o.id === orderId);

    if (!order) {
        return res.status(404).json({ success: false, error: 'الطلب غير موجود' });
    }

    const validStatuses = ['new', 'preparing', 'ready', 'delivered', 'cancelled'];
    const previousStatus = order.status;

    if (status) {
        if (!validStatuses.includes(status)) {
            return res.status(400).json({ success: false, error: 'حالة غير صالحة' });
        }

        order.status = status;
        console.log(`📝 تحديث #${order.id}: ${status}`);

        // ✅ إشعار عند الجاهزية - إرسال WebSocket
        if (status === 'ready' && previousStatus !== 'ready') {
            const orderTypeMsg = {
                'dine_in': 'يمكنك استلامه من الكاونتر',
                'car_pickup': 'سنوصله لسيارتك الآن',
                'delivery': 'جاري توصيله إليك'
            };

            const notificationMessage = `🎉 تم تجهيز طلبك #${order.id}! ${orderTypeMsg[order.orderType] || ''}`;

            order.readyNotification = {
                sent: true,
                message: notificationMessage,
                timestamp: new Date().toISOString()
            };

            console.log(`🔔 إشعار جاهز للعميل: ${order.customerName} - طلب #${order.id}`);

            // ✅ إرسال الإشعار عبر WebSocket لجميع العملاء المتصلين
            manager.broadcast({
                type: 'order_ready',
                orderId: order.id,
                message: notificationMessage,
                orderType: order.orderType,
                customerName: order.customerName,
                timestamp: new Date().toISOString()
            });

            // إرسال للمتابعين المحددين لهذا الطلب
            manager.sendToOrder(order.id, {
                type: 'order_ready',
                orderId: order.id,
                message: notificationMessage,
                orderType: order.orderType,
                timestamp: new Date().toISOString()
            });
        }
    }

    if (notes !== undefined) {
        order.notes = notes;
    }

    order.updatedAt = new Date().toISOString();

    res.json({ success: true, order });
});

// حذف طلب
app.delete('/api/orders/:orderId', (req, res) => {
    const orderId = parseInt(req.params.orderId);
    const orderIndex = db.orders.findIndex(o => o.id === orderId);

    if (orderIndex === -1) {
        return res.status(404).json({ success: false, error: 'الطلب غير موجود' });
    }

    const deleted = db.orders.splice(orderIndex, 1)[0];
    console.log(`🗑️ حذف #${deleted.id}`);

    res.json({ success: true, message: 'تم حذف الطلب' });
});

// إحصائيات الطلبات
app.get('/api/stats', (req, res) => {
    autoCleanup();

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const todayOrders = db.orders.filter(o => new Date(o.createdAt) >= today);
    const deliveredToday = todayOrders.filter(o => o.status === 'delivered');

    res.json({
        success: true,
        stats: {
            total: db.orders.length,
            today: todayOrders.length,
            todayRevenue: deliveredToday.reduce((sum, o) => sum + o.total, 0),
            byStatus: {
                new: db.orders.filter(o => o.status === 'new').length,
                preparing: db.orders.filter(o => o.status === 'preparing').length,
                ready: db.orders.filter(o => o.status === 'ready').length,
                delivered: db.orders.filter(o => o.status === 'delivered').length,
                cancelled: db.orders.filter(o => o.status === 'cancelled').length
            },
            byType: {
                dine_in: db.orders.filter(o => o.orderType === 'dine_in').length,
                car_pickup: db.orders.filter(o => o.orderType === 'car_pickup').length,
                delivery: db.orders.filter(o => o.orderType === 'delivery').length
            }
        }
    });
});

// Long polling للتحديثات
app.get('/api/orders/poll', async (req, res) => {
    const since = parseInt(req.query.since) || 1000;
    const newOrders = db.orders.filter(o => o.id > since);

    if (newOrders.length > 0) {
        const lastId = Math.max(...newOrders.map(o => o.id));
        return res.json({
            hasUpdates: true,
            orders: newOrders,
            lastId
        });
    }

    await new Promise(resolve => setTimeout(resolve, 1000));
    res.json({ hasUpdates: false, lastId: since });
});

// إشعارات الطلبات الجاهزة
app.get('/api/notifications/ready', (req, res) => {
    try {
        let sinceTime;
        if (req.query.since) {
            const sinceClean = req.query.since.replace('Z', '').split('+')[0].split('.')[0];
            sinceTime = new Date(sinceClean);
        } else {
            sinceTime = new Date(Date.now() - 60000); // آخر دقيقة
        }

        const readyOrders = db.orders.filter(o => {
            if (o.status === 'ready' && o.readyNotification) {
                try {
                    const notifTimeStr = o.readyNotification.timestamp.split('+')[0].split('.')[0];
                    const notifTime = new Date(notifTimeStr);
                    return notifTime > sinceTime;
                } catch {
                    return false;
                }
            }
            return false;
        });

        res.json({
            success: true,
            notifications: readyOrders.map(o => ({
                orderId: o.id,
                message: o.readyNotification.message,
                timestamp: o.readyNotification.timestamp,
                orderType: o.orderType || 'dine_in'
            }))
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// فحص صحة السيرفر
app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        server: 'ملك الطابون - Backend',
        version: '3.1.0',
        orders: db.orders.length,
        websocket_connections: manager.allConnections.length,
        uptime: 'running'
    });
});

// مسح جميع الطلبات
app.delete('/api/cleanup', (req, res) => {
    const count = db.orders.length;
    db.orders = [];
    db.counter = 1000;
    console.log(`🧹 تم مسح ${count} طلب`);
    res.json({ success: true, message: `تم مسح ${count} طلب` });
});

// ═══════════════════════════════════════════════════════════════════════════
// 🚀 تشغيل السيرفر
// ═══════════════════════════════════════════════════════════════════════════

server.listen(PORT, () => {
    // بدء المسح اليومي
    dailyCleanupAt5am();

    console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║                                                                           ║
║   🔥  ملك الطابون - Backend المركزي v3.1 (Node.js + WebSocket)           ║
║                                                                           ║
║   📡 السيرفر يعمل على: http://localhost:${PORT}                             ║
║                                                                           ║
║   ═══════════════════════════════════════════════════════════════════════ ║
║                                                                           ║
║   🔌 APIs للموقع الأول (العملاء):                                         ║
║      POST /api/chat         → محادثة مع الذكاء الاصطناعي                  ║
║      WS   /ws/notifications → إشعارات فورية للعملاء ✨ جديد               ║
║      WS   /ws/notifications?orderId=X → متابعة طلب محدد ✨                 ║
║                                                                           ║
║   📦 APIs للموقع الثاني (العمال):                                         ║
║      GET  /api/orders       → جلب كل الطلبات                              ║
║      GET  /api/orders?orderType=delivery → فلترة حسب النوع                 ║
║      POST /api/orders       → إنشاء طلب يدوي                              ║
║      PATCH /api/orders/:id  → تحديث حالة طلب (يرسل WebSocket)             ║
║      DELETE /api/orders/:id → حذف طلب                                     ║
║      GET  /api/stats        → الإحصائيات                                  ║
║      GET  /api/orders/poll  → تحديثات فورية                               ║
║                                                                           ║
║   ✅ أنواع الطلبات المدعومة:                                              ║
║      🏠 dine_in     → داخل المحل                                          ║
║      🚗 car_pickup  → بالسيارة                                            ║
║      🛵 delivery    → توصيل للمنزل                                        ║
║                                                                           ║
║   ❤️  GET /api/health        → فحص صحة السيرفر                             ║
║                                                                           ║
║   جاهز لخدمة الموقعين! 🚀                                                 ║
║                                                                           ║
╚═══════════════════════════════════════════════════════════════════════════╝
    `);
});