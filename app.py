# app.py
import os
import time
import json
import pandas as pd
from flask import Flask, jsonify, send_from_directory, request
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from werkzeug.middleware.proxy_fix import ProxyFix
import re
from datetime import datetime, timedelta
import threading
import logging
import pyttsx3
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import numpy as np

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# 配置
EXCEL_FILE_PATH = '计划安排进度表.xlsx'
JSON_OUTPUT_PATH = 'progress_data.json'
SETTINGS_PATH = 'alert_settings.json'
CACHE_TIMEOUT = 30  # 数据缓存时间(秒)
ALERT_DATA_PATH = 'alert_data.json'

# 默认设置
DEFAULT_SETTINGS = {
    'afternoon_alert_time': "13:59",   # 提前一日预警时间（前一天）
    'morning_alert_time': "00:00",     # 当日预警时间（当天）
    'last_modified': 0
}

# 内存缓存
data_cache = {
    'timestamp': 0,
    'data': None,
    'periods': None,
    'alerts': []  # 存储预警信息
}

# 存储当前活动的预警 
active_alerts = {}
scheduler = BackgroundScheduler()

# 加载设置
def load_settings():
    try:
        if os.path.exists(SETTINGS_PATH):
            with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                settings = json.load(f)
                # 验证设置
                if 'afternoon_alert_time' in settings and 'morning_alert_time' in settings:
                    return settings
    except:
        pass
    return DEFAULT_SETTINGS.copy()

# 保存设置
def save_settings(settings):
    try:
        with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
            json.dump(settings, f, ensure_ascii=False, indent=4)
        return True
    except:
        return False

# 初始化设置
alert_settings = load_settings()

def safe_convert_excel():
    """安全转换Excel文件，带错误恢复机制"""
    try:
        # 尝试多种读取方式
        logging.debug('开始尝试读取Excel文件')
        try:
            df_all = pd.read_excel(
                EXCEL_FILE_PATH,
                sheet_name='进度表（6.3~6.16）',
                header=None,
                engine='openpyxl'
            )
            logging.debug('使用openpyxl成功读取Excel文件')
        except Exception as e:
            logging.warning(f'使用openpyxl读取Excel文件失败: {e}，尝试其他方式')
            df_all = pd.read_excel(
                EXCEL_FILE_PATH,
                sheet_name='进度表（6.3~6.16）',
                header=None
            )
            logging.debug('使用其他方式成功读取Excel文件')
        
        # 提取周期信息
        periods = {
            'plan_period': "2025年6月-2025年10月",
            'department': "研发部",
            'project': "AMS",
            'progress_period': "2025-06-17 至 2025-06-27",
            'last_period': "2025-06-03 至 2025-06-16"
        }
        
        try:
            if df_all is not None and len(df_all) > 1:
                row1 = df_all.iloc[1].tolist()
                if len(row1) > 2:
                    start = str(row1[1]) if not pd.isna(row1[1]) else "2025年6月"
                    end = str(row1[2]) if not pd.isna(row1[2]) else "2025年10月"
                    periods['plan_period'] = f"{start} - {end}"
                if len(row1) > 4 and not pd.isna(row1[4]):
                    periods['department'] = str(row1[4])
                if len(row1) > 6 and not pd.isna(row1[6]):
                    periods['project'] = str(row1[6])
            
            if df_all is not None and len(df_all) > 2:
                row2 = df_all.iloc[2].tolist()
                if len(row2) > 2:
                    start_date = str(row2[1]) if not pd.isna(row2[1]) else "2025-06-17"
                    end_date = str(row2[2]) if not pd.isna(row2[2]) else "2025-06-27"
                    periods['progress_period'] = f"{start_date} - {end_date}"
                if len(row2) > 5:
                    last_start = str(row2[4]).split()[0] if not pd.isna(row2[4]) else "2025-06-03"
                    last_end = str(row2[5]).split()[0] if not pd.isna(row2[5]) else "2025-06-16"
                    periods['last_period'] = f"{last_start} - {last_end}"
        except Exception as e:
            pass
        
        # 提取项目数据（从第4行开始）
        try:
            df = pd.read_excel(
                EXCEL_FILE_PATH,
                sheet_name='进度表（6.3~6.16）',
                header=3,
                usecols="A:N",  # 包含预警日期和预警列
                engine='openpyxl'
            )
        except Exception as e:
            df = pd.read_excel(
                EXCEL_FILE_PATH,
                sheet_name='进度表（6.3~6.16）',
                header=3
            )
            df = df.iloc[:, :14]  # 确保只取前14列（包含预警日期和预警列）
        
        # 列名处理（新增alert_date和alert_content列）
        df.columns = [
            'id', 'client', 'project_name', 'product_name', 'classification',
            'delivery_date', 'responsible', 'workshop_progress', 'drawing',
            'software', 'simulation', 'listing', 'alert_date', 'alert_content'
        ]
        
        # 数据清洗
        valid_rows = []
        current_id = 1
        def get_first(x):
            if isinstance(x, (list, tuple, np.ndarray, pd.Series)):
                return x[0]
            return x
        def isna_all(x):
            v = pd.isna(x)
            if isinstance(v, (np.ndarray, pd.Series)):
                return v.all()
            return v
        for _, row in df.iterrows():
            client = get_first(row['client'])
            if isinstance(client, str) and ('计划出货时间' in client or '出货日期计划安排表' in client):
                continue
            if isna_all(client) or str(client).strip() == '':
                continue
            valid_row = {
                'id': current_id,
                'client': client if not isna_all(client) else '',
                'project_name': get_first(row['project_name']) if not isna_all(get_first(row['project_name'])) else '',
                'product_name': get_first(row['product_name']) if not isna_all(get_first(row['product_name'])) else '',
                'classification': get_first(row['classification']) if not isna_all(get_first(row['classification'])) else '',
                'delivery_date': get_first(row['delivery_date']) if not isna_all(get_first(row['delivery_date'])) else '',
                'responsible': get_first(row['responsible']) if not isna_all(get_first(row['responsible'])) else '',
                'workshop_progress': get_first(row['workshop_progress']) if not isna_all(get_first(row['workshop_progress'])) else '',
                'drawing': get_first(row['drawing']) if not isna_all(get_first(row['drawing'])) else 0,
                'software': get_first(row['software']) if not isna_all(get_first(row['software'])) else 0,
                'simulation': get_first(row['simulation']) if not isna_all(get_first(row['simulation'])) else 0,
                'listing': get_first(row['listing']) if not isna_all(get_first(row['listing'])) else 0,
                'alert_date': get_first(row['alert_date']) if not isna_all(get_first(row['alert_date'])) else '',
                'alert_content': get_first(row['alert_content']) if not isna_all(get_first(row['alert_content'])) else ''
            }
            # 添加调试日志
            logging.debug(f"读取项目: ID={current_id}, 预警日期={valid_row['alert_date']}, 预警内容={valid_row['alert_content']}")
            valid_rows.append(valid_row)
            current_id += 1
        
        # 数值类型转换
        for row in valid_rows:
            for col in ['drawing', 'software', 'simulation', 'listing']:
                try:
                    value = row[col]
                    # 只对list/tuple/np.ndarray/pd.Series取第一个元素
                    if isinstance(value, (list, tuple, np.ndarray, pd.Series)):
                        value = value[0]
                    if isinstance(value, str):
                        numbers = re.findall(r'\d+', value)
                        if numbers:
                            value = int(numbers[0])
                        else:
                            value = 0
                    else:
                        value = int(float(value))
                    row[col] = max(0, min(100, value))
                except (ValueError, TypeError, IndexError):
                    row[col] = 0
        
        return valid_rows, periods
    except Exception as e:
        logging.error(f"Excel转换错误: {str(e)}")
        return None, None

# 增强日期解析函数
def parse_alert_date(date_str):
    if pd.isna(date_str) or date_str == '' or str(date_str).strip().lower() in ['待定', 'none', 'null', 'nan']:
        return None
    
    date_str = str(date_str).strip()
    logging.debug(f"原始预警日期: {date_str}")
    
    # 尝试多种日期格式
    formats = [
        '%Y.%m.%d',    # 2025.06.24
        '%Y-%m-%d',    # 2025-06-24
        '%Y/%m/%d',    # 2025/06/24
        '%Y年%m月%d日', # 2025年06月24日
        '%Y.%m.%d',    # 2025.6.24 (带一位数月份)
        '%m/%d/%Y',    # 06/24/2025 (美国格式)
        '%d/%m/%Y'     # 24/06/2025 (欧洲格式)
    ]
    
    for fmt in formats:
        try:
            # 尝试解析
            parsed_date = datetime.strptime(date_str, fmt).date()
            logging.debug(f"成功解析日期: {date_str} -> {parsed_date}")
            return parsed_date
        except ValueError:
            continue
    
    # 尝试正则表达式匹配
    patterns = [
        r'(\d{4})[\.\-\/](\d{1,2})[\.\-\/](\d{1,2})',  # 2025.06.24
        r'(\d{1,2})[\.\-\/](\d{1,2})[\.\-\/](\d{4})',  # 24.06.2025
        r'(\d{4})年(\d{1,2})月(\d{1,2})日'             # 2025年06月24日
    ]
    
    for pattern in patterns:
        match = re.match(pattern, date_str)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3:
                    # 处理不同格式
                    if len(groups[0]) == 4:  # 年份在前
                        year = int(groups[0])
                        month = int(groups[1])
                        day = int(groups[2])
                    else:  # 日期在前
                        day = int(groups[0])
                        month = int(groups[1])
                        year = int(groups[2])
                    
                    # 处理两位年份
                    if year < 100:
                        if year > 50:
                            year += 1900
                        else:
                            year += 2000
                    
                    parsed_date = datetime(year, month, day).date()
                    logging.debug(f"通过正则解析日期: {date_str} -> {parsed_date}")
                    return parsed_date
            except Exception as e:
                logging.error(f"日期解析错误: {date_str}, 错误: {str(e)}")
                continue
    
    logging.error(f"无法解析日期: {date_str}")
    return None

def should_trigger_alert(project, settings): 
    alert_date = parse_alert_date(project['alert_date']) 
    if not alert_date: 
        return False 
    
    # 获取当前日期和时间 
    today = datetime.now().date() 
    now = datetime.now() 
    
    # 检查是否是提前一天预警（前一天）或当天预警（当天） 
    alert_date_afternoon = alert_date - timedelta(days=1)  # 提前一天预警 
    alert_date_morning = alert_date  # 当天预警 
    
    # 检查当前日期是否匹配（提前一天或当天） 
    if today != alert_date_afternoon and today != alert_date_morning: 
        return False 
    
    # 根据当前日期是提前一天还是当天，决定使用哪个预警时间段 
    if today == alert_date_afternoon: 
        # 提前一天预警，使用下午时间段 
        alert_time_str = settings.get('afternoon_alert_time', '13:59') 
    else: 
        # 当天预警，使用上午时间段 
        alert_time_str = settings.get('morning_alert_time', '00:00') 
    
    try: 
        alert_hour, alert_minute = map(int, alert_time_str.split(':')) 
        # 创建预警时间对象 
        alert_time = datetime( 
            now.year, now.month, now.day, 
            alert_hour, alert_minute 
        ) 
        
        # 如果当前时间在预警时间之后 
        return now >= alert_time 
    except Exception as e: 
        logging.error(f"时间检查错误: {str(e)}") 
        return False 

def check_alerts(project_data):
    """检查需要预警的项目"""
    alerts = []
    today = datetime.now().date()
    
    for project in project_data:
        # 确保项目名称、预警日期和预警内容都不为空
        project_name = str(project['project_name']).strip()
        alert_date_str = str(project['alert_date']).strip()
        alert_content = str(project['alert_content']).strip()
        
        # 检查关键字段是否为空
        if not project_name or not alert_date_str or not alert_content:
            continue
            
        if should_trigger_alert(project, alert_settings):
            # 只收集有预警内容的项目
            if alert_content and alert_content != "待定":
                alert_data = {
                    'id': project['id'],
                    'project_name': project_name,
                    'alert_content': alert_content,
                    'alert_date': alert_date_str,
                    'expiry_date': today.strftime("%Y-%m-%d")
                }
                alerts.append(alert_data)
                logging.info(f"触发预警: {project['id']} - {project_name} - 预警日期: {alert_date_str}")
            else:
                logging.info(f"跳过预警（内容无效）: {project['id']} - {project_name} - 内容: {alert_content}")
    
    return alerts

def update_active_alerts(alerts):
    """更新活跃预警列表"""
    global active_alerts
    current_time = time.time()
    # 获取当前有效的预警ID
    current_alert_ids = {str(alert['id']) for alert in alerts}

    # 移除已不在当前预警列表中的项目
    expired_ids = [aid for aid in active_alerts if aid not in current_alert_ids]
    for alert_id in expired_ids:
          logging.info(f"移除过期预警: {alert_id}")
          del active_alerts[alert_id]

    # 添加新预警或更新已有预警
    for alert in alerts:
        alert_id = str(alert['id'])
        # 如果已存在，则更新；否则添加
        if alert_id in active_alerts:
            # 更新预警数据
            active_alerts[alert_id]['data'] = alert
            active_alerts[alert_id]['created_at'] = current_time
            active_alerts[alert_id]['expiry_date'] = datetime.now().date().strftime("%Y-%m-%d")
            logging.info(f"更新预警: {alert['project_name']}")
        else:
            active_alerts[alert_id] = {
                'data': alert,
                'created_at': current_time,
                'expiry_date': datetime.now().date().strftime("%Y-%m-%d")
            }
            logging.info(f"添加新预警: {alert['project_name']}")
    
    # 保存当前活跃预警
    try:
        with open(ALERT_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(list(active_alerts.values()), f, ensure_ascii=False, indent=4)
    except Exception as e:
        logging.error(f"保存预警数据错误: {str(e)}")

def update_cache():
    """更新缓存数据"""
    current_time = time.time()
    if current_time - data_cache['timestamp'] > CACHE_TIMEOUT or data_cache['data'] is None:
        new_data, periods = safe_convert_excel()
        if new_data is not None and periods is not None:
            # 检查预警项目
            alerts = check_alerts(new_data)
            data_cache['alerts'] = alerts
            
            # 更新活跃预警
            update_active_alerts(alerts)
            
            data_cache['data'] = new_data
            data_cache['periods'] = periods
            data_cache['timestamp'] = current_time
            
            # 异步保存到文件
            try:
                with open(JSON_OUTPUT_PATH, 'w', encoding='utf-8') as f:
                    json.dump({
                        'projects': new_data,
                        'periods': periods,
                        'alerts': alerts,
                        'active_alerts': [alert['data'] for alert in active_alerts.values()]
                    }, f, ensure_ascii=False, indent=4)
            except Exception as e:
                logging.error(f"保存JSON错误: {str(e)}")
    return data_cache['data'], data_cache['periods'], data_cache['alerts']

def load_active_alerts():
    """从文件加载活跃预警"""
    global active_alerts
    try:
        if os.path.exists(ALERT_DATA_PATH):
            with open(ALERT_DATA_PATH, 'r', encoding='utf-8') as f:
                alerts = json.load(f)
                for alert in alerts:
                    alert_id = str(alert['data']['id'])
                    active_alerts[alert_id] = alert
                logging.info(f"已加载 {len(active_alerts)} 条活跃预警")
    except Exception as e:
        logging.error(f"加载预警数据错误: {str(e)}")

class ExcelFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(EXCEL_FILE_PATH):
            logging.info("Excel文件已修改，更新缓存")
            update_cache()
            # 不再调用trigger_alert，避免Excel一保存就语音播报

@app.route('/api/data', methods=['GET'])
def get_progress():
    try:
        data, periods, alerts = update_cache()
        if data is None:
            data = []
        if periods is None:
            periods = {}
        if alerts is None:
            alerts = []
        
        # 返回活跃预警
        active_alerts_list = [alert['data'] for alert in active_alerts.values()]
        
        return jsonify({
            'status': 'success',
            'data': data,
            'periods': periods,
            'alerts': alerts,
            'active_alerts': active_alerts_list,
            'timestamp': data_cache['timestamp'],
            'alert_settings': alert_settings
        })
    except Exception as e:
        # 返回缓存中的旧数据
        cached_data = data_cache['data'] or []
        cached_periods = data_cache['periods'] or {}
        cached_alerts = data_cache['alerts'] or []
        active_alerts_list = [alert['data'] for alert in active_alerts.values()]
        
        return jsonify({
            'status': 'error',
            'message': str(e),
            'data': cached_data,
            'periods': cached_periods,
            'alerts': cached_alerts,
            'active_alerts': active_alerts_list,
            'alert_settings': alert_settings,
            'cached': True
        }), 500

@app.route('/api/save_settings', methods=['POST'])
def save_alert_settings():
    try:
        data = request.json
        afternoon_alert_time = data.get('afternoon_alert_time')
        morning_alert_time = data.get('morning_alert_time')
        
        # 验证时间格式
        try:
            # 验证下午时间 (13:00-23:59) 
            hour, minute = map(int, afternoon_alert_time.split(':'))
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return jsonify({'status': 'error', 'message': '时间必须在00:00-23:59之间'}), 400
            
            # 验证上午时间 (00:00-23:59) 
            hour, minute = map(int, morning_alert_time.split(':'))
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                return jsonify({'status': 'error', 'message': '时间必须在00:00-23:59之间'}), 400
        except:
            return jsonify({'status': 'error', 'message': '时间格式无效'}), 400
        
        # 更新设置
        global alert_settings
        alert_settings['afternoon_alert_time'] = afternoon_alert_time
        alert_settings['morning_alert_time'] = morning_alert_time
        alert_settings['last_modified'] = time.time()
        
        # 保存到文件
        if save_settings(alert_settings):
            # 刷新缓存
            update_cache()
            
            # 重新设置定时任务
            setup_alert_jobs()
            
            return jsonify({'status': 'success', 'message': '设置已保存'})
        else:
            return jsonify({'status': 'error', 'message': '保存设置失败'}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'cache_age': time.time() - data_cache['timestamp']
    })

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def static_file(path):
    return send_from_directory('.', path)

def start_file_monitor():
    event_handler = ExcelFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=False)
    observer.start()
    return observer

# 语音播报函数
def voice_alert(message):
    if not message:
        return
        
    engine = pyttsx3.init()
    for _ in range(2):  # 播报两次
        engine.say(message)
        engine.runAndWait()

def trigger_alert(mode):
    """
    mode: 'afternoon'（前一天）或 'morning'（当天）
    每次定时点只播报该定时点应播报的预警，无论是否已播报过。
    """
    update_cache()
    now = datetime.now()
    today = now.date()
    alerts_to_broadcast = []
    for project in data_cache['data']:
        alert_date = parse_alert_date(project['alert_date'])
        alert_content = str(project.get('alert_content', '')).strip()
        project_name = str(project.get('project_name', '')).strip()
        if not alert_date or not alert_content or not project_name:
            continue
        if mode == 'afternoon' and today == alert_date - timedelta(days=1):
            alerts_to_broadcast.append(project)
        elif mode == 'morning' and today == alert_date:
            alerts_to_broadcast.append(project)
    if alerts_to_broadcast:
        alert_message = ""
        for alert in alerts_to_broadcast:
            alert_message += f"{alert['project_name']}，{alert['alert_date']}，{alert['alert_content']}。"
        if alert_message:
            logging.info(f"[{mode}] 播报警报: {alert_message}")
            voice_alert(alert_message)
    else:
        logging.info(f"[{mode}] 当前没有需要播报的预警")

# 设置定时任务
def setup_alert_jobs():
    """
    只保留前一天和当天两个定时任务，分别调用trigger_alert('afternoon')和trigger_alert('morning')。
    重新设置预警时间后立即生效。
    """
    global scheduler, alert_settings
    scheduler.remove_all_jobs()
    afternoon_time = alert_settings['afternoon_alert_time'].split(':')
    morning_time = alert_settings['morning_alert_time'].split(':')
    scheduler.add_job(
        lambda: trigger_alert('afternoon'),
        trigger=CronTrigger(
            hour=int(afternoon_time[0]),
            minute=int(afternoon_time[1])
        ),
        id='afternoon_alert'
    )
    scheduler.add_job(
        lambda: trigger_alert('morning'),
        trigger=CronTrigger(
            hour=int(morning_time[0]),
            minute=int(morning_time[1])
        ),
        id='morning_alert'
    )
    logging.info(f"已设置预警任务: 前一天 {alert_settings['afternoon_alert_time']}，当天 {alert_settings['morning_alert_time']}")

if __name__ == '__main__': 
    # 加载活跃预警 
    load_active_alerts() 
    
    # 预热缓存 
    update_cache() 
    
    # 启动Excel文件监听，自动刷新缓存和预警
    start_file_monitor()
    
    # 配置并启动调度器
    scheduler.start()
    setup_alert_jobs()
    
    try: 
        app.run( 
            host='0.0.0.0', 
            port=5000, 
            threaded=True, 
            debug=True, 
            use_reloader=False 
        ) 
    finally: 
        scheduler.shutdown()