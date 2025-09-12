import requests, re, logging
from logger import setup_logger
from nas import upload_bytes_to_nas
from dotenv import load_dotenv

# 환경변수 불러오기
load_dotenv()

def download_and_upload():
    setup_logger()

    url = "https://www.energy.or.kr/commonFile/fileDownload.do"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.energy.or.kr",
        "Referer": "https://www.energy.or.kr/front/board/View9.do",
        "User-Agent": "Mozilla/5.0",
        "Cookie": "_ga=GA1.1.1353667739.1757641526; _ga_XBK150XKCK=GS2.1; _xm_webid_1=2059651657; JSESSIONID=c0dLyA5Iix1vbXGVmKPNa0w8seo6PekWEavp5NwozdVmktU4lwSPATP3WAQANf58.amV1c19kb21haW4vRU5FUkdZ"
    }

    files = [
        {"fileNo": "24158", "fileSeq": "1", "boardMngNo": "9"},
        {"fileNo": "24156", "fileSeq": "2", "boardMngNo": "9"},
        {"fileNo": "24155", "fileSeq": "3", "boardMngNo": "9"},
        {"fileNo": "20645", "fileSeq": "15239", "boardMngNo": "9"}, 
        {"fileNo": "20741", "fileSeq": "15572", "boardMngNo": "9"},
        {"fileNo": "24154", "fileSeq": "19304", "boardMngNo": "9"},
        {"fileNo": "23670", "fileSeq": "18616", "boardMngNo": "9"},
        {"fileNo": "23399", "fileSeq": "18682", "boardMngNo": "9"},
        {"fileNo": "23064", "fileSeq": "18047", "boardMngNo": "9"},
        {"fileNo": "22808", "fileSeq": "17882", "boardMngNo": "9"},
        {"fileNo": "22774", "fileSeq": "17865", "boardMngNo": "9"},
        {"fileNo": "22299", "fileSeq": "17521", "boardMngNo": "9"},
        {"fileNo": "22078", "fileSeq": "17165", "boardMngNo": "9"},
        {"fileNo": "22051", "fileSeq": "17133", "boardMngNo": "9"},
        {"fileNo": "21772", "fileSeq": "16578", "boardMngNo": "9"},
        {"fileNo": "21760", "fileSeq": "19301", "boardMngNo": "9"},
        {"fileNo": "21380", "fileSeq": "15998", "boardMngNo": "9"},
        {"fileNo": "21311", "fileSeq": "15920", "boardMngNo": "9"},
        {"fileNo": "21184", "fileSeq": "15666", "boardMngNo": "9"},
        {"fileNo": "20847", "fileSeq": "15536", "boardMngNo": "9"},
        {"fileNo": "20741", "fileSeq": "15572", "boardMngNo": "9"},
        {"fileNo": "20645", "fileSeq": "15239", "boardMngNo": "9"},
        {"fileNo": "20220", "fileSeq": "14424", "boardMngNo": "9"},
        {"fileNo": "20063", "fileSeq": "14311", "boardMngNo": "9"},
        {"fileNo": "19415", "fileSeq": "13275", "boardMngNo": "9"},
        {"fileNo": "18908", "fileSeq": "12545", "boardMngNo": "9"},
    ]


    for fdata in files:
        r = requests.post(url, headers=headers, data=fdata)

        # Content-Disposition 헤더에서 파일명 추출
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r'filename="?([^"]+)"?', cd)

        if m:
            raw_name = m.group(1)
            try:
                filename = raw_name.encode("latin1").decode("cp949")
            except Exception:
                filename = raw_name
        else:
            filename = f"report_{fdata['fileNo']}_{fdata['fileSeq']}.pdf"

        logging.info(f"다운로드 완료: {filename} ({len(r.content)} bytes)")

        try:
            # NAS 업로드 (메모리에서 바로 업로드)
            server_path = upload_bytes_to_nas(r.content, filename)
            logging.info(f"NAS 업로드 완료: {server_path}")
        except Exception as e:
            logging.error(f"NAS 업로드 실패: {filename} | {e}")

if __name__ == "__main__":
    download_and_upload()