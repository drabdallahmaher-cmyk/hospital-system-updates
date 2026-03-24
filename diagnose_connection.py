#!/usr/bin/env python3
"""
تشخيص حالة الاتصال بقاعدة البيانات
Database Connection Diagnostic Tool
"""
import sys
import os
import json

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("=" * 70)
print("🔍 أداة تشخيص الاتصال بقاعدة البيانات")
print("=" * 70)

# Test 1: Check config.json
print("\n[Test 1] التحقق من ملف الإعدادات config.json")
print("-" * 70)

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
if os.path.exists(config_path):
    print(f"✅ ملف config.json موجود: {config_path}")
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        print(f"\n📋 إعدادات PostgreSQL:")
        print(f"  • الخادم (Host): {config.get('db_host', 'غير محدد')}")
        print(f"  • قاعدة البيانات (DB Name): {config.get('db_name', 'غير محدد')}")
        print(f"  • المستخدم (User): {config.get('user', 'غير محدد')}")
        print(f"  • كلمة المرور (Password): {'*' * len(str(config.get('password', '')))}")
        print(f"  • المنفذ (Port): {config.get('port', 'غير محدد')}")
        print(f"  • SSL Mode: {config.get('sslmode', 'غير محدد')}")
        
        # Check if password is default/placeholder
        password = config.get('password', '')
        if password in ['your_password_here', '', None]:
            print(f"\n⚠️ تحذير: كلمة المرور هي القيمة الافتراضية!")
            print(f"   يجب تغييرها للاتصال الفعلي.")
        else:
            print(f"\n✅ كلمة المرور محددة")
            
    except Exception as e:
        print(f"\n❌ خطأ في قراءة config.json: {e}")
else:
    print(f"❌ ملف config.json غير موجود")

# Test 2: Internet Connectivity
print("\n[Test 2] التحقق من الاتصال بالإنترنت")
print("-" * 70)

try:
    from MAIN import check_internet_connection
    
    internet_status = check_internet_connection(timeout=5)
    
    if internet_status:
        print("✅ الإنترنت: متصل")
        print("   ✓ يمكن الوصول إلى Google DNS (8.8.8.8)")
        print("   ✓ يمكن الوصول إلى Cloudflare DNS (1.1.1.1)")
    else:
        print("❌ الإنترنت: غير متصل")
        print("   ✗ فشل الاتصال بخوادم DNS")
        
except Exception as e:
    print(f"❌ فشل اختبار الإنترنت: {e}")

# Test 3: DatabaseManager Status
print("\n[Test 3] التحقق من حالة DatabaseManager")
print("-" * 70)

try:
    from MAIN import DatabaseManager
    
    db = DatabaseManager()
    
    # Check internet only
    internet_only = db.check_internet_connection()
    print(f"الاتصال بالإنترنت: {'✅ متصل' if internet_only else '❌ غير متصل'}")
    
    # Check full online status (Internet + PostgreSQL)
    is_online = db.is_online()
    print(f"حالة النظام الكاملة: {'✅ متصل (Online)' if is_online else '⚠️ غير متصل'}")
    
    if not is_online and internet_only:
        print(f"\n📊 التشخيص:")
        print(f"   ✓ الإنترنت متصل")
        print(f"   ✗ PostgreSQL غير متصل")
        print(f"\n🔍 الأسباب المحتملة:")
        print(f"   1. كلمة مرور خاطئة")
        print(f"   2. خادم PostgreSQL غير مشغّل")
        print(f"   3. إعدادات خاطئة في config.json")
        print(f"   4. الجدار الناري يمنع الاتصال")
        
    elif is_online:
        print(f"\n🎉 كل شيء يعمل بشكل صحيح!")
        print(f"   ✓ الإنترنت متصل")
        print(f"   ✓ PostgreSQL متصل")
        
except Exception as e:
    print(f"❌ فشل اختبار DatabaseManager: {e}")
    import traceback
    traceback.print_exc()

# Test 4: Sync Status
print("\n[Test 4] حالة المزامنة Sync Status")
print("-" * 70)

try:
    sync_status = db.get_sync_status()
    
    print(f"العناصر المعلقة (Pending): {sync_status['pending']}")
    print(f"قيد المعالجة (Processing): {sync_status['processing']}")
    print(f"الفاشلة (Failed): {sync_status['failed']}")
    print(f"المنجزة (Done): {sync_status['done']}")
    print(f"آخر مزامنة: {sync_status['last_sync']}")
    print(f"حالة الاتصال: {sync_status['is_online']}")
    
    if sync_status['failed'] > 0:
        print(f"\n⚠️ تنبيه: توجد {sync_status['failed']} عمليات مزامنة فاشلة")
        print("   سيتم إعادة المحاولة تلقائياً عند تحسين الاتصال")
        
except Exception as e:
    print(f"❌ فشل اختبار المزامنة: {e}")

# Test 5: SQLite Local Database
print("\n[Test 5] قاعدة البيانات المحلية SQLite")
print("-" * 70)

try:
    import sqlite3
    from MAIN import LOCAL_DB_PATH
    
    if os.path.exists(LOCAL_DB_PATH):
        print(f"✅ قاعدة البيانات المحلية موجودة")
        print(f"   الموقع: {LOCAL_DB_PATH}")
        
        conn = sqlite3.connect(LOCAL_DB_PATH)
        cursor = conn.cursor()
        
        # Count patients
        cursor.execute("SELECT COUNT(*) FROM patients")
        patient_count = cursor.fetchone()[0]
        print(f"   عدد المرضى: {patient_count}")
        
        # Count visits
        cursor.execute("SELECT COUNT(*) FROM visits")
        visit_count = cursor.fetchone()[0]
        print(f"   عدد الزيارات: {visit_count}")
        
        conn.close()
        print(f"\n✅ SQLite يعمل بشكل صحيح")
    else:
        print(f"⚠️ قاعدة البيانات المحلية غير موجودة (سيتم إنشاؤها)")
        
except Exception as e:
    print(f"❌ فشل اختبار SQLite: {e}")

# Final Summary
print("\n" + "=" * 70)
print("📊 ملخص التشخيص")
print("=" * 70)

try:
    if internet_only and not is_online:
        print("""
🟡 الحالة: متصل بالإنترنت (PostgreSQL غير مهيأ)

التشخيص:
✓ الإنترنت يعمل بشكل صحيح
✗ لا يمكن الاتصال بـ PostgreSQL

الإجراءات المطلوبة:
1. راجع ملف DATABASE_CONFIG_GUIDE.md
2. تحقق من صحة إعدادات PostgreSQL في config.json
3. تأكد من تشغيل خادم PostgreSQL
4. اختبر الاتصال مرة أخرى

النظام يعمل حالياً في الوضع المحلي فقط.
جميع البيانات تحفظ في SQLite حتى يتم تكوين PostgreSQL.
""")
        
    elif is_online:
        print("""
🟢 الحالة: متصل (Online)

التشخيص:
✓ الإنترنت متصل
✓ PostgreSQL متصل
✓ المزامنة تعمل بشكل طبيعي

النظام يعمل بكامل طاقته! 🎉
""")
        
    else:
        print("""

🔴 الحالة: غير متصل (Offline)

التشخيص:
✗ لا يوجد اتصال بالإنترنت

الإجراءات المطلوبة:
1. تحقق من اتصال الشبكة/WiFi
2. تأكد من عمل الإنترنت على الجهاز
3. راجع إعدادات الراوتر/الشبكة

النظام يعمل في الوضع المحلي فقط حتى يتم استعادة الإنترنت.
""")

except:
    print("\n⚠️ تعذر تحديد الحالة النهائية")

print("=" * 70)
print("تم التشخيص بنجاح ✅")
print("=" * 70)
