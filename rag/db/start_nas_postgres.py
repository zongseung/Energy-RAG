#!/usr/bin/env python3
"""
NAS PostgreSQL 서버 시작 스크립트
"""
import subprocess
import time
import os
import psycopg
from pathlib import Path

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

def start_postgres_server():
    """NAS PostgreSQL 서버 시작"""
    try:
        # PostgreSQL 데이터 디렉토리
        data_dir = "/home/user/rag/naverDB/database"
        
        print(f"🚀 PostgreSQL 서버 시작 중... (데이터 디렉토리: {data_dir})")
        
        # postgres 사용자 권한 확인 및 서버 시작
        cmd = [
            "sudo", "-u", "postgres", 
            "/usr/lib/postgresql/15/bin/pg_ctl",
            "-D", data_dir,
            "-l", f"{data_dir}/postgresql.log",
            "start"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ PostgreSQL 서버가 시작되었습니다.")
            print(f"📝 로그: {data_dir}/postgresql.log")
            return True
        else:
            print(f"❌ PostgreSQL 서버 시작 실패:")
            print(f"   stdout: {result.stdout}")
            print(f"   stderr: {result.stderr}")
            
            # 직접 postgres 실행 시도
            print("\n🔄 직접 postgres 실행 시도...")
            direct_cmd = [
                "sudo", "-u", "postgres",
                "/usr/lib/postgresql/15/bin/postgres",
                "-D", data_dir,
                "-p", "5433"  # 다른 포트 사용
            ]
            
            print(f"실행 명령: {' '.join(direct_cmd)}")
            print("⚠️  이 명령은 백그라운드에서 실행됩니다. Ctrl+C로 중단하세요.")
            
            # 백그라운드에서 실행
            process = subprocess.Popen(direct_cmd)
            print(f"🔄 프로세스 ID: {process.pid}")
            
            # 몇 초 대기 후 연결 테스트
            time.sleep(5)
            return test_connection("localhost", 5433)
            
    except Exception as e:
        print(f"❌ 서버 시작 중 오류: {e}")
        return False

def test_connection(host="localhost", port=5432):
    """PostgreSQL 연결 테스트"""
    try:
        conn_str = f"postgresql://postgres@{host}:{port}/naver"
        print(f"🔍 연결 테스트: {conn_str}")
        
        with psycopg.connect(conn_str, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print(f"✅ 연결 성공! PostgreSQL 버전: {version}")
                return True
                
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return False

def show_postgres_processes():
    """실행 중인 postgres 프로세스 확인"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        postgres_lines = [line for line in result.stdout.split('\n') if 'postgres' in line]
        
        if postgres_lines:
            print("\n📊 실행 중인 PostgreSQL 프로세스:")
            for line in postgres_lines:
                print(f"   {line}")
        else:
            print("\n❌ 실행 중인 PostgreSQL 프로세스가 없습니다.")
            
    except Exception as e:
        print(f"❌ 프로세스 확인 중 오류: {e}")

def check_port_usage():
    """포트 사용 상황 확인"""
    try:
        result = subprocess.run(["netstat", "-tlnp"], capture_output=True, text=True)
        postgres_ports = [line for line in result.stdout.split('\n') if ':543' in line]
        
        if postgres_ports:
            print("\n🔌 PostgreSQL 관련 포트 사용 상황:")
            for line in postgres_ports:
                print(f"   {line}")
        else:
            print("\n❌ PostgreSQL 포트(5432-5434)가 사용되지 않고 있습니다.")
            
    except Exception as e:
        print(f"❌ 포트 확인 중 오류: {e}")

def main():
    print("🏗️ NAS PostgreSQL 서버 시작 도구")
    print("=" * 50)
    
    # 1. NAS 마운트 확인
    if not check_nas_mount():
        return False
    
    # 2. 현재 상태 확인
    show_postgres_processes()
    check_port_usage()
    
    # 3. PostgreSQL 서버 시작
    if start_postgres_server():
        print("\n🎉 PostgreSQL 서버가 성공적으로 시작되었습니다!")
        
        # 4. 연결 테스트
        print("\n🔍 연결 테스트 중...")
        for port in [5432, 5433, 5434]:
            if test_connection("localhost", port):
                print(f"✅ 포트 {port}에서 연결 성공!")
                break
        else:
            print("❌ 모든 포트에서 연결 실패")
            
        return True
    else:
        print("\n💥 PostgreSQL 서버 시작에 실패했습니다.")
        
        print("\n💡 수동 해결 방법:")
        print("1. sudo systemctl start postgresql")
        print("2. sudo -u postgres pg_ctl -D /home/user/rag/naverDB/database start")
        print("3. 로그 확인: tail -f /home/user/rag/naverDB/database/postgresql.log")
        
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

