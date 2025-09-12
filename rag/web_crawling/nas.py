# energy_scraper/nas.py
import os
from ftplib import FTP, error_perm
from typing import Optional
from io import BytesIO 

def _resolve_config(dest_dir: Optional[str] = None):
    """
    호출 시점에 환경변수를 읽어 FTP 설정을 구성합니다.
    dest_dir가 주어지면 NAS_FOLDER를 덮어씁니다.
    """
    NAS_IP = os.getenv("NAS_IP")
    FTP_PORT = int(os.getenv("FTP_PORT", "21"))
    USERNAME = os.getenv("NAS_USERNAME")
    PASSWORD = os.getenv("NAS_PASSWORD")
    NAS_FOLDER = dest_dir if dest_dir is not None else os.getenv("NAS_FOLDER", "/")

    if not NAS_IP or not USERNAME or not PASSWORD:
        raise ValueError("NAS 접속 정보가 누락되었습니다. NAS_IP / NAS_USERNAME / NAS_PASSWORD 를 확인하세요.")
    return NAS_IP, FTP_PORT, USERNAME, PASSWORD, NAS_FOLDER

def _ftp_makedirs(ftp: FTP, path: str):
    """
    FTP 상에 중첩 디렉터리 생성. 존재하면 건너뜀.
    """
    parts = [p for p in path.split("/") if p != ""]
    if path.startswith("/"):
        ftp.cwd("/")
    for part in parts:
        try:
            ftp.cwd(part)
        except error_perm:
            ftp.mkd(part)
            ftp.cwd(part)

def upload_bytes_to_nas(file_bytes: bytes, file_name: str, *, dest_dir: Optional[str] = None,
                        timeout: int = 60, retries: int = 2, passive: bool = True) -> str:
    """
    로컬 파일 저장 없이, 메모리에 있는 데이터를 바로 NAS로 업로드합니다.
    """
    NAS_IP, FTP_PORT, USERNAME, PASSWORD, NAS_FOLDER = _resolve_config(dest_dir)

    last_err = None
    for attempt in range(retries + 1):
        try:
            ftp = FTP(timeout=timeout)
            ftp.encoding = "utf-8"

            ftp.connect(NAS_IP, FTP_PORT)
            ftp.login(USERNAME, PASSWORD)
            ftp.set_pasv(passive)

            _ftp_makedirs(ftp, NAS_FOLDER)

            bio = BytesIO(file_bytes)
            ftp.storbinary(f"STOR {file_name}", bio)

            ftp.quit()
            server_path = (NAS_FOLDER.rstrip("/") or "/") + "/" + file_name
            return server_path

        except Exception as e:
            last_err = e
            try:
                ftp.quit()
            except:
                pass
            if attempt < retries:
                continue
            break

    raise RuntimeError(f"NAS 업로드 실패: {file_name} -> {NAS_FOLDER} | {last_err}")
