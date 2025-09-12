#!/usr/bin/env python3
"""
로컬 PostgreSQL → NAS PostgreSQL 마이그레이션 도구
"""
import subprocess
import time
import psycopg
from pathlib import Path
import os

def check_nas_mount():
    """NAS 마운트 상태 확인"""
    nas_mount = Path("/home/user/rag/naverDB")
    if not nas_mount.exists():
        print("❌ NAS가 마운트되지 않았습니다.")
        return False
    
    db_path = nas_mount / "database"
    if not db_path.exists():
        print("❌ NAS 데이터베이스 디렉토리가 없습니다.")
        return False
    
    print(f"✅ NAS 마운트 확인: {nas_mount}")
    return True

def start_nas_postgres():
    """NAS PostgreSQL 서버 시작"""
    print("🚀 NAS PostgreSQL 서버 시작 중...")
    
    # NAS에서 PostgreSQL 서버 시작 명령어
    start_commands = [
        # 방법 1: 직접 postgres 실행
        "sudo -u postgres /usr/lib/postgresql/15/bin/postgres -D /volume1/naverResearch/database -p 5432",
        # 방법 2: 다른 포트로 실행
        "sudo -u postgres /usr/lib/postgresql/15/bin/postgres -D /volume1/naverResearch/database -p 5433",
        # 방법 3: pg_ctl 사용
        "sudo -u postgres /usr/lib/postgresql/15/bin/pg_ctl -D /volume1/naverResearch/database -l /tmp/postgres.log start"
    ]
    
    print("💡 NAS에서 다음 명령어 중 하나를 실행하세요:")
    for i, cmd in enumerate(start_commands, 1):
        print(f"   {i}. {cmd}")
    
    print("\n⚠️  백그라운드 실행을 위해서는 명령어 끝에 '&'를 추가하세요")
    print("   예: sudo -u postgres /usr/lib/postgresql/15/bin/postgres -D /volume1/naverResearch/database -p 5432 &")
    
    return True

def test_nas_connection(nas_ip="192.9.66.151", ports=[5432, 5433, 5434]):
    """NAS PostgreSQL 연결 테스트"""
    print(f"🔍 NAS PostgreSQL 연결 테스트 중... (IP: {nas_ip})")
    
    for port in ports:
        try:
            conn_str = f"postgresql://postgres@{nas_ip}:{port}/naver"
            print(f"   포트 {port} 테스트 중...")
            
            with psycopg.connect(conn_str, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    print(f"✅ 포트 {port} 연결 성공! PostgreSQL 버전: {version}")
                    return port
                    
        except Exception as e:
            print(f"❌ 포트 {port} 연결 실패: {e}")
    
    return None

def backup_local_db():
    """로컬 DB 백업"""
    print("💾 로컬 PostgreSQL 데이터베이스 백업 중...")
    
    backup_dir = Path("db/backups")
    backup_dir.mkdir(exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"naver_backup_{timestamp}.sql"
    
    # Docker 컨테이너에서 pg_dump 실행
    cmd = [
        "docker", "exec", "rag-postgres_db1-1",
        "pg_dump", "-U", "zongseung", "-d", "naver",
        "--verbose", "--clean", "--if-exists", "--create"
    ]
    
    try:
        with open(backup_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            file_size = backup_file.stat().st_size / 1024 / 1024
            print(f"✅ 로컬 백업 완료: {backup_file} ({file_size:.2f} MB)")
            return backup_file
        else:
            print(f"❌ 백업 실패: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ 백업 중 오류: {e}")
        return None

def restore_to_nas(backup_file, nas_ip, port):
    """NAS에 데이터 복원"""
    print(f"🔄 NAS에 데이터 복원 중... (IP: {nas_ip}, 포트: {port})")
    
    try:
        # NAS에 백업 파일 복사
        nas_backup_dir = Path("/home/user/rag/naverDB/db_backups")
        nas_backup_dir.mkdir(exist_ok=True)
        
        nas_backup_file = nas_backup_dir / backup_file.name
        subprocess.run(["cp", str(backup_file), str(nas_backup_file)], check=True)
        print(f"✅ 백업 파일 NAS 복사 완료: {nas_backup_file}")
        
        # NAS에서 데이터베이스 복원
        restore_cmd = [
            "psql", "-h", nas_ip, "-p", str(port), "-U", "postgres", "-d", "postgres",
            "-f", str(nas_backup_file)
        ]
        
        print(f"🔄 복원 명령어: {' '.join(restore_cmd)}")
        print("⚠️  이 명령어를 NAS에서 실행하거나, 로컬에서 실행하세요.")
        
        # 환경변수 설정 (비밀번호 없이)
        env = os.environ.copy()
        env['PGPASSWORD'] = ''  # postgres 사용자는 비밀번호 없음
        
        result = subprocess.run(restore_cmd, env=env, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ NAS 데이터 복원 완료!")
            return True
        else:
            print(f"❌ 복원 실패: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 복원 중 오류: {e}")
        return False

def verify_migration(nas_ip, port):
    """마이그레이션 검증"""
    print("🔍 마이그레이션 검증 중...")
    
    try:
        conn_str = f"postgresql://postgres@{nas_ip}:{port}/naver"
        
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                # 테이블 개수 확인
                cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
                table_count = cur.fetchone()[0]
                print(f"✅ 테이블 개수: {table_count}")
                
                # documents 테이블 레코드 수 확인
                cur.execute("SELECT COUNT(*) FROM documents;")
                doc_count = cur.fetchone()[0]
                print(f"✅ documents 레코드 수: {doc_count}")
                
                # 샘플 데이터 확인
                cur.execute("SELECT id, category, filename FROM documents LIMIT 5;")
                samples = cur.fetchall()
                print("✅ 샘플 데이터:")
                for sample in samples:
                    print(f"   ID: {sample[0]}, Category: {sample[1]}, File: {sample[2]}")
                
                return True
                
    except Exception as e:
        print(f"❌ 검증 실패: {e}")
        return False

def main():
    print("🏗️ 로컬 PostgreSQL → NAS PostgreSQL 마이그레이션 도구")
    print("=" * 60)
    
    # 1. NAS 마운트 확인
    if not check_nas_mount():
        return False
    
    # 2. NAS PostgreSQL 서버 시작 안내
    print("\n📋 1단계: NAS PostgreSQL 서버 시작")
    start_nas_postgres()
    
    input("\n⏸️  NAS에서 PostgreSQL 서버를 시작한 후 Enter를 누르세요...")
    
    # 3. NAS 연결 테스트
    print("\n📋 2단계: NAS 연결 테스트")
    nas_port = test_nas_connection()
    
    if not nas_port:
        print("❌ NAS PostgreSQL 연결에 실패했습니다.")
        print("💡 해결 방법:")
        print("   1. NAS에서 PostgreSQL 서버가 실행 중인지 확인")
        print("   2. 방화벽 설정 확인")
        print("   3. 포트 번호 확인")
        return False
    
    # 4. 로컬 DB 백업
    print(f"\n📋 3단계: 로컬 DB 백업 (NAS 포트: {nas_port})")
    backup_file = backup_local_db()
    
    if not backup_file:
        print("❌ 로컬 DB 백업에 실패했습니다.")
        return False
    
    # 5. NAS에 데이터 복원
    print("\n📋 4단계: NAS에 데이터 복원")
    if restore_to_nas(backup_file, "192.9.66.151", nas_port):
        print("✅ 데이터 복원 완료!")
    else:
        print("❌ 데이터 복원에 실패했습니다.")
        return False
    
    # 6. 마이그레이션 검증
    print("\n📋 5단계: 마이그레이션 검증")
    if verify_migration("192.9.66.151", nas_port):
        print("\n🎉 마이그레이션이 성공적으로 완료되었습니다!")
        print(f"📊 NAS PostgreSQL 연결 정보:")
        print(f"   호스트: 192.9.66.151")
        print(f"   포트: {nas_port}")
        print(f"   데이터베이스: naver")
        print(f"   사용자: postgres")
        return True
    else:
        print("❌ 마이그레이션 검증에 실패했습니다.")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

