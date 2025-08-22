import time
import pandas as pd
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logs_path = "logs"

def read_file_with_retry(file_path, retries=3, delay=3):
    """Đọc file Excel với retry để xử lý lỗi file đang được sử dụng."""
    for attempt in range(retries):
        try:
            return pd.read_excel(file_path)
        except Exception as e:
            print(f"[{time.ctime()}] Retry {attempt + 1}/{retries} for {file_path}: {e}")
            with open(os.path.join(logs_path, 'merge_error.log'), 'a') as log_file:
                log_file.write(f"[{time.ctime()}] Retry {attempt + 1}/{retries} for {file_path}: {e}\n")
            time.sleep(delay)
    raise Exception(f"[{time.ctime()}] Failed to read {file_path} after {retries} attempts")


def is_file_stable(file_path, check_interval=1, max_checks=5):
    """Kiểm tra file có ổn định (không còn được ghi) không."""
    if not os.path.exists(file_path):
        return False
    initial_size = os.path.getsize(file_path)
    for _ in range(max_checks):
        time.sleep(check_interval)
        if not os.path.exists(file_path):
            return False
        current_size = os.path.getsize(file_path)
        if current_size == initial_size:
            return True
        initial_size = current_size
    return False


def extract_hr_code(file_name):
    """Trích xuất mã HR từ tên file (ví dụ: HR_myduyen_ly.xlsx -> myduyen_ly)."""
    base_name = os.path.splitext(file_name)[0]  # Loại bỏ .xlsx
    if base_name.startswith('HR_'):
        return base_name[3:]  # Bỏ tiền tố HR_
    return base_name  # Trả về tên không có tiền tố nếu không có HR_


def merge_hr_files(file_path):
    """Gộp file HR vào file master, điền SSO từ tên file nếu thiếu."""
    hr_files_dir = "hr_files"
    output_dir = "update"
    output_file = os.path.join(output_dir, "master_data_updated.xlsx")

    # Kiểm tra file có phải là .xlsx và không phải file tạm
    if not file_path.endswith('.xlsx') or os.path.basename(file_path).startswith('~$'):
        print(f"[{time.ctime()}] Skipping invalid file: {file_path}")
        return

    # Kiểm tra file ổn định
    if not is_file_stable(file_path):
        print(f"[{time.ctime()}] File {file_path} is not stable, skipping.")
        return

    # Trích xuất mã HR từ tên file
    hr_code = extract_hr_code(os.path.basename(file_path))
    print(f"[{time.ctime()}] Processing file: {file_path}, HR code: {hr_code}")

    # Đọc file HR
    try:
        hr_data = read_file_with_retry(file_path)
        if 'SSO' not in hr_data.columns:
            print(f"[{time.ctime()}] Warning: File {file_path} does not contain 'SSO' column. Adding SSO column.")
            hr_data['SSO'] = hr_code
        else:
            # Ghi log các hàng thiếu SSO
            missing_SSO = hr_data['SSO'].isna() | (hr_data['SSO'] == '')
            if missing_SSO.any():
                print(f"[{time.ctime()}] Found {missing_SSO.sum()} rows with missing SSO in {file_path}")
                with open(os.path.join(logs_path, 'missing_SSO_log.txt'), 'a') as log_file:
                    log_file.write(f"[{time.ctime()}] File {file_path}: {missing_SSO.sum()} rows missing SSO\n")
                    log_file.write(f"Rows with missing SSO:\n{hr_data[missing_SSO][['EID']].to_string()}\n")
                # Điền SSO từ tên file
                hr_data.loc[missing_SSO, 'SSO'] = hr_code
        print(f"[{time.ctime()}] Reading file: {file_path} with {len(hr_data)} rows")
    except Exception as e:
        print(f"[{time.ctime()}] Error reading file {file_path}: {e}")
        with open(os.path.join(logs_path, 'merge_error.log'), 'a') as log_file:
            log_file.write(f"[{time.ctime()}] Error reading {file_path}: {e}\n")
        return

    # Đọc file master hiện tại (nếu tồn tại)
    master_data = pd.DataFrame()
    if os.path.exists(output_file):
        try:
            master_data = read_file_with_retry(output_file)
            print(f"[{time.ctime()}] Loaded existing master data with {len(master_data)} rows")
        except Exception as e:
            print(f"[{time.ctime()}] Error reading master data {output_file}: {e}")
            with open(os.path.join(logs_path, 'merge_error.log'), 'a') as log_file:
                log_file.write(f"[{time.ctime()}] Error reading {output_file}: {e}\n")

    # Loại bỏ các hàng trong master data có 'EID' trùng với hr_data
    if 'EID' in hr_data.columns and not master_data.empty and 'EID' in master_data.columns:
        master_data = master_data[~master_data['EID'].isin(hr_data['EID'])]
        print(f"[{time.ctime()}] Removed duplicate rows from master for EID")

    # Gộp dữ liệu
    merged_data = pd.concat([master_data, hr_data], ignore_index=True)

    # Xử lý trùng lặp
    if 'EID' in merged_data.columns:
        merged_data = merged_data.drop_duplicates(subset=['EID'], keep='last')
        print(f"[{time.ctime()}] Rows after removing duplicates by 'EID': {len(merged_data)}")
    else:
        print(f"[{time.ctime()}] Warning: 'EID' not found. Using 'SSO' to check for duplicates.")
        merged_data = merged_data.drop_duplicates(subset=['SSO'], keep='last')

    # Lưu file master
    try:
        merged_data.to_excel(output_file, index=False)
        print(f"[{time.ctime()}] Master data updated and saved to: {output_file} with {len(merged_data)} rows")
        with open(os.path.join(logs_path, 'merge_log.txt'), 'a') as log_file:
            log_file.write(
                f"[{time.ctime()}] Merged {len(hr_data)} rows from {file_path} (HR: {hr_code}) into {output_file} ({len(merged_data)} total rows)\n")
    except Exception as e:
        print(f"[{time.ctime()}] Error saving master data to {output_file}: {e}")
        with open(os.path.join(logs_path, 'merge_error.log'), 'a') as log_file:
            log_file.write(f"[{time.ctime()}] Error saving {output_file}: {e}\n")


class HRFileWatcher(FileSystemEventHandler):
    def on_any_event(self, event):
        """Ghi lại tất cả sự kiện để debug."""
        if not event.is_directory:
            print(f"[{time.ctime()}] Event: {event.event_type}, Path: {event.src_path}")
            with open(os.path.join(logs_path, 'watchdog_log.txt'), 'a') as log_file:
                log_file.write(f"[{time.ctime()}] Event: {event.event_type}, Path: {event.src_path}\n")

    def on_modified(self, event):
        """Xử lý khi file được chỉnh sửa."""
        if not event.is_directory and event.src_path.endswith('.xlsx'):
            if os.path.basename(event.src_path).startswith('~$'):
                main_file = event.src_path.replace('~$', '')
                if os.path.exists(main_file):
                    print(
                        f"[{time.ctime()}] Detected temporary file: {event.src_path}, checking main file: {main_file}")
                    time.sleep(1)
                    if is_file_stable(main_file):
                        merge_hr_files(main_file)
            else:
                print(f"[{time.ctime()}] Detected modification: {event.src_path}")
                time.sleep(1)
                if is_file_stable(event.src_path):
                    merge_hr_files(event.src_path)

    def on_created(self, event):
        """Xử lý khi file được tạo mới."""
        if not event.is_directory and event.src_path.endswith('.xlsx') and not os.path.basename(
                event.src_path).startswith('~$'):
            print(f"[{time.ctime()}] Detected new file: {event.src_path}")
            time.sleep(1)
            if is_file_stable(event.src_path):
                merge_hr_files(event.src_path)


if __name__ == "__main__":
    hr_files_dir = "hr_files"
    if not os.path.exists(hr_files_dir):
        raise FileNotFoundError(f"Directory {hr_files_dir} does not exist")

    event_handler = HRFileWatcher()
    observer = Observer()
    observer.schedule(event_handler, hr_files_dir, recursive=False)
    observer.start()
    print(f"[{time.ctime()}] Monitoring {hr_files_dir} directory for changes...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print(f"[{time.ctime()}] Stopped monitoring")
    observer.join()