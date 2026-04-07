import os
import sys
import subprocess

def main():
    # 1. التأكد من وجود المجلدات الضرورية
    data_dir = os.environ.get("DATA_DIR", "/data")
    os.makedirs(data_dir, exist_ok=True)
    
    # 2. جلب المنفذ من Railway
    port = os.environ.get("PORT", "8501")
    
    # 3. تشغيل Streamlit بشكل صحيح
    print(f"[Boot] Starting Hybrid System on Port {port}...")
    cmd = [
        sys.executable, "-m", "streamlit", "run", "app.py",
        "--server.port", port,
        "--server.address", "0.0.0.0",
        "--server.headless", "true"
    ]
    
    # استخدام subprocess.run لضمان بقاء العملية نشطة
    result = subprocess.run(cmd)
    sys.exit(result.returncode)

if __name__ == "__main__":
    main()
