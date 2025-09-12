#!/usr/bin/env python3
import subprocess
import datetime
from pathlib import Path

def backup_to_nas():
    """로컬 DB를 백업해서 NAS에 저장"""
    try:
        # 백업 디렉토리 생성
        local_backup_dir = Path("db/backups")
        nas_backup_dir = Path("/home/user/rag/naverDB/db_backups")
        
        local_backup_dir.mkdir(exist_ok=True)
        nas_backup_dir.mkdir(exist_ok=True)
        
        # 타임스탬프
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = local_backup_dir / f"naver_backup_{timestamp}.sql"
        
        print(f"🔄 로컬 DB 백업 중... → {backup_file}")
        
        # pg_dump 실행
        cmd = [
            "docker", "exec", "rag-postgres_db1-1",
            "pg_dump", "-U", "zongseung", "-d", "naver",
            "--verbose", "--clean", "--if-exists"
        ]
        
        with open(backup_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            file_size = backup_file.stat().st_size / 1024 / 1024
            print(f"✅ 로컬 백업 완료: {backup_file} ({file_size:.2f} MB)")
            
            # NAS에 복사
            nas_backup_file = nas_backup_dir / backup_file.name
            subprocess.run(["cp", str(backup_file), str(nas_backup_file)], check=True)
            print(f"✅ NAS 백업 완료: {nas_backup_file}")
            
            return True
        else:
            print(f"❌ 백업 실패: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 백업 중 오류: {e}")
        return False

if __name__ == "__main__":
    print("💾 로컬 DB → NAS 백업 시작")
    success = backup_to_nas()
    
    if success:
        print("🎉 백업 완료!")
    else:
        print("💥 백업 실패!")
