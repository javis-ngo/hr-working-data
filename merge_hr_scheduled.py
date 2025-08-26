import os
import time
import pandas as pd
import logging
import schedule
from filelock import FileLock
from datetime import datetime

from logic import write_preserving_formulas_and_styles

# Cấu hình logging
logs_path = "logs"
os.makedirs(logs_path, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_path, 'merge.log')),
        logging.StreamHandler()
    ]
)

SKIP_MASTER = 11
SHEET_MASTER = "Masterdata_PSteam"
keep_format_columns = [
        "SENIORITY", "PROBATION CONTRACT NO", "FROM", "TO", "DEFINITE CONTRACT 1 NO", "FROM", "TO",
        "DEFINITE CONTRACT 2 NO", "FROM", "TO", "IN-DEFINITE CONTRACT  NO", "FROM",
        "END EMPLOYMENT DATE", "BASE SALARY", "COMPLEXCITY ALLOWANCE", "POSITION ALLOWANCE", "Language allowance", "Total amount contribute SHUI", "MEAL ALLOWANCE", "SUBTOTAL", "SHUI FROM", "SHUI TO"
    ]


def read_file_with_retry(file_path, retries=5, initial_delay=1):
    """Đọc file Excel với cơ chế retry và backoff."""
    for attempt in range(retries):
        try:
            return pd.read_excel(file_path, skiprows=11)  # Đọc toàn bộ cột
        except Exception as e:
            delay = initial_delay * (2 ** attempt)  # Exponential backoff
            logging.error(f"Retry {attempt + 1}/{retries} for {file_path}: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to read {file_path} after {retries} attempts")


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
    base_name = os.path.splitext(file_name)[0]
    return base_name[3:] if base_name.startswith('HR_') else base_name


def validate_excel_schema(df, required_columns=['EID']):
    """Kiểm tra schema của file có chứa các cột bắt buộc."""
    return all(col in df.columns for col in required_columns)


def merge_hr_files():
    """Gộp tất cả file HR trong thư mục hr_files và hợp nhất với master_data."""
    hr_files_dir = "hr_files"
    output_dir = "update"
    master_updated_file = os.path.join(output_dir, "master_data_updated.xlsx")
    master_origin_file = "master_data.xlsx"
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()
    merged_data = pd.DataFrame()

    # Quét tất cả file .xlsx trong thư mục hr_files
    for file_name in os.listdir(hr_files_dir):
        file_path = os.path.join(hr_files_dir, file_name)
        if not file_name.endswith('.xlsx') or file_name.startswith('~$'):
            logging.info(f"Skipping invalid file: {file_path}")
            continue

        if not is_file_stable(file_path):
            logging.warning(f"File {file_path} is not stable, skipping")
            continue

        hr_code = extract_hr_code(file_name)
        logging.info(f"Processing file: {file_path}, HR code: {hr_code}")

        try:
            hr_data = read_file_with_retry(file_path)
            if not validate_excel_schema(hr_data):
                logging.error(f"Invalid schema in {file_path}")
                continue

            if 'SSO' not in hr_data.columns:
                hr_data['SSO'] = hr_code
            else:
                missing_sso = hr_data['SSO'].isna() | (hr_data['SSO'] == '')
                if missing_sso.any():
                    logging.warning(f"Found {missing_sso.sum()} rows with missing SSO in {file_path}")
                    hr_data.loc[missing_sso, 'SSO'] = hr_code

            merged_data = pd.concat([merged_data, hr_data], ignore_index=True)
            logging.info(f"Merged {len(hr_data)} rows from {file_path}")
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue

    # Loại bỏ trùng lặp trong merged_data
    if 'EID' in merged_data.columns:
        merged_data = merged_data.drop_duplicates(subset=['EID'], keep='last')
        logging.info(f"Rows after removing duplicates: {len(merged_data)}")

    # Lưu file master_data_updated
    lock_file = f"{master_updated_file}.lock"
    with FileLock(lock_file):
        write_preserving_formulas_and_styles(
            template_path=master_origin_file,  # file template gốc
            output_path=master_updated_file,  # file đích
            df=merged_data,  # dữ liệu đã merge
            sheet_name=SHEET_MASTER,  # tên sheet trong template
            header_skip_rows=SKIP_MASTER,  # số row skip trước header
            keep_formula_columns=keep_format_columns  # cột cần giữ công thức
        )
        try:
            logging.info(f"Saved master_data_updated with {len(merged_data)} rows")
        except Exception as e:
            logging.error(f"Error saving {master_updated_file}: {e}")
            return

    # Hợp nhất với master_data
    if os.path.exists(master_origin_file):
        try:
            origin_data = read_file_with_retry(master_origin_file)
            # Loại bỏ các EID trong origin_data có trong merged_data
            if 'EID' in origin_data.columns and 'EID' in merged_data.columns:
                origin_data = origin_data[~origin_data['EID'].isin(merged_data['EID'])]
            final_data = pd.concat([origin_data, merged_data], ignore_index=True)
            final_data = final_data.drop_duplicates(subset=['EID'], keep='last')

            # Lưu lại master_data
            with FileLock(lock_file):
                write_preserving_formulas_and_styles(
                    template_path=master_origin_file,  # file template gốc
                    output_path=master_origin_file,  # file đích
                    df=final_data,  # dữ liệu đã merge
                    sheet_name=SHEET_MASTER,  # tên sheet trong template
                    header_skip_rows=SKIP_MASTER,  # số row skip trước header
                    keep_formula_columns=keep_format_columns  # cột cần giữ công thức
                )
                # final_data.to_excel(master_updated_file, index=False)
                logging.info(f"Final merged data saved with {len(final_data)} rows")
        except Exception as e:
            logging.error(f"Error merging with {master_origin_file}: {e}")
    else:
        logging.warning(f"Don't merge data with {master_origin_file}")
    duration = time.time() - start_time
    logging.info(f"Completed merge in {duration:.2f} seconds")


def schedule_merge():
    time_schedule="14:38"
    schedule.every().day.at(time_schedule).do(merge_hr_files)
    logging.info(f"Scheduled merge job at {time_schedule} AM daily")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Kiểm tra mỗi phút


if __name__ == "__main__":
    if not os.path.exists("hr_files"):
        raise FileNotFoundError("Directory hr_files does not exist")
    schedule_merge()