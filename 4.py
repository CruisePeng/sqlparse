import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import sqlparse
import pandas as pd
import pymysql
import warnings
import re
from typing import Dict, List, Tuple
import json
import os
from urllib.parse import quote_plus, unquote_plus

warnings.filterwarnings('ignore')
# ========== 配置常量 ==========
HISTORY_FILE = "db_connection_history.txt"  # 历史连接存储文件
URL_FORMAT = "{db_type}://{user}:{password}@{host}:{port}/{database}"  # URL拼接格式
# ========== 数据库连接类（优化版） ==========
class DBConnector:
    def __init__(self, db_type, host, port, user, password, database=None):
        self.db_type = db_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    # 端口校验 连接建立
    def connect(self):
        # 端口输入校验
        try:
            port_int = int(self.port)
        except ValueError:
            messagebox.showerror("输入错误", "端口必须是数字！")
            return False

        try:
            if self.db_type == "MySQL":
                self.conn = pymysql.connect(
                    host=self.host,
                    port=port_int,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    charset='utf8mb4',  # 兼容更多字符
                    connect_timeout=10
                )
            # Hive连接（注释：需额外装依赖，先测试MySQL）
            # elif self.db_type == "Hive":
            #     from pyhive import hive
            #     self.conn = hive.Connection(
            #         host=self.host,
            #         port=port_int,
            #         username=self.user,
            #         password=self.password,
            #         database=self.database
            #     )
            self.cursor = self.conn.cursor()
            return True
        except pymysql.Error as e:
            messagebox.showerror("MySQL连接失败", f"错误码：{e.args[0]}，信息：{e.args[1]}")
            return False
        except Exception as e:
            messagebox.showerror("连接失败", f"未知错误：{str(e)}")
            return False

    # 执行SQL（增加行数限制，避免卡顿）
    def execute_sql(self, sql, limit_rows=1000, disable_limit=False):
        count_sql = None
        try:
            # 自动加行数限制，防止大数据量崩溃
            if not sql.strip().upper().endswith("LIMIT") and "LIMIT" not in sql.upper():
                sql += f" LIMIT {limit_rows}"

            print(f"执行SQL：{sql}")
            self.cursor.execute(sql)

            # 获取列名
            columns = [desc[0] for desc in self.cursor.description]
            # 获取数据
            data = self.cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            return df
        except pymysql.Error as e:
            messagebox.showerror("执行失败", f"MySQL错误：{e.args[0]} - {e.args[1]}")
            return None
        except Exception as e:
            messagebox.showerror("执行失败", f"SQL执行出错：{str(e)}")
            return None

    def get_total_count(self, sql):
        """
        根据给定的 SQL 查询总记录数
        :param sql: 原始 SQL 查询语句
        :return: 总记录数（int）或 None（如果执行失败）
        """
        try:
            # 构造 COUNT(*) 查询
            count_sql = f"SELECT COUNT(*) AS total_count FROM ({sql}) AS subquery"

            # 执行查询
            self.cursor.execute(count_sql)

            # 获取结果
            result = self.cursor.fetchone()
            total_count = result[0] if result else 0

            return total_count
        except pymysql.Error as e:
            messagebox.showerror("执行失败", f"MySQL错误：{e.args[0]} - {e.args[1]}")
            return None
        except Exception as e:
            messagebox.showerror("执行失败", f"SQL执行出错：{str(e)}")
            return None

    # 关闭连接
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


# ========== 历史连接管理工具 ==========
class DBHistoryManager:
    @staticmethod
    def load_history():
        """加载历史连接信息，返回字典 {url: config}"""
        history = {}
        if not os.path.exists(HISTORY_FILE):
            return history

        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                for line in f.readlines():
                    line = line.strip()
                    if not line:
                        continue
                    config = json.loads(line)
                    url = DBHistoryManager._generate_url(config)
                    history[url] = config
        except Exception as e:
            messagebox.showerror("加载失败", f"读取历史连接失败：{str(e)}")
        return history

    @staticmethod
    def save_history(config):
        """保存连接信息到历史文件（去重）"""
        config = {k: v for k, v in config.items() if v is not None and v != ""}
        if not config.get("host") or not config.get("port") or not config.get("user"):
            return

        new_url = DBHistoryManager._generate_url(config)
        history = DBHistoryManager.load_history()
        history[new_url] = config  # 去重更新

        try:
            with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
                for config in history.values():
                    f.write(json.dumps(config, ensure_ascii=False) + "\n")
        except Exception as e:
            messagebox.showerror("保存失败", f"保存历史连接失败：{str(e)}")

    @staticmethod
    def _generate_url(config):
        """生成唯一URL（密码URL编码避免特殊字符）"""
        return URL_FORMAT.format(
            db_type=config.get("db_type", "MySQL"),
            user=quote_plus(config.get("user", "")),
            password=quote_plus(config.get("password", "")),
            host=config.get("host", ""),
            port=config.get("port", ""),
            database=quote_plus(config.get("database", ""))
        )

    @staticmethod
    def parse_url(url):
        """解析URL为配置字典"""
        try:
            db_type, rest = url.split("://", 1)
            user_pass, host_db = rest.split("@", 1)
            user, password = user_pass.split(":", 1)
            host_port, database = host_db.split("/", 1)
            host, port = host_port.split(":", 1)

            return {
                "db_type": db_type,
                "user": unquote_plus(user),
                "password": unquote_plus(password),
                "host": host,
                "port": port,
                "database": unquote_plus(database)
            }
        except Exception as e:
            messagebox.showerror("解析失败", f"URL解析失败：{str(e)}")
            return {}







# ========== SQL解析类（优化版） ==========
class SQLParser:
    @staticmethod
    def parse_cte_sql(sql: str) -> List[Tuple[str, str]]:
        """
        解析 WITH CTE，返回有序列表：
        [
          (cte_name, cte_sql),
          ...
        ]
        """
        sql = sql.strip()
        match = re.search(r'\bwith\b', sql, re.IGNORECASE)
        if not match:
            return []

        pos = match.end()
        length = len(sql)
        ctes = []

        while pos < length:
            # 跳过空白和逗号
            while pos < length and sql[pos] in " \n\t,":
                pos += 1

            # 读取 CTE 名称
            name_match = re.match(r'([a-zA-Z_][a-zA-Z0-9_]*)', sql[pos:])
            if not name_match:
                break

            cte_name = name_match.group(1)
            pos += name_match.end()

            # 跳过空白
            while pos < length and sql[pos].isspace():
                pos += 1

            # 必须是 AS (
            if not re.match(r'(?i)as\s*\(', sql[pos:]):
                break

            # 找到 "("
            pos = sql.lower().find("(", pos)
            start = pos + 1

            # 括号匹配
            bracket_count = 1
            pos += 1
            while pos < length and bracket_count > 0:
                if sql[pos] == "(":
                    bracket_count += 1
                elif sql[pos] == ")":
                    bracket_count -= 1
                pos += 1

            end = pos - 1
            cte_sql = sql[start:end].strip()

            ctes.append((cte_name, cte_sql))

            # 判断是否还有下一个 CTE
            while pos < length and sql[pos].isspace():
                pos += 1
            if pos >= length or sql[pos] != ",":
                break

        return ctes

    def build_executable_cte_sql(ctes: List[Tuple[str, str]]) -> Dict[str, str]:
        """
        构建「可执行 SQL」：
        每个 CTE 都包含之前所有 CTE 定义
        """
        result = {}
        accumulated = []

        for name, sql_body in ctes:
            accumulated.append((name, sql_body))

            with_parts = []
            for n, s in accumulated:
                with_parts.append(f"{n} AS (\n{s}\n)")

            full_sql = (
                    "WITH\n" +
                    ",\n".join(with_parts) +
                    f"\nSELECT * FROM {name}"
            )

            result[name] = full_sql

        return result


# ========== 主GUI界面（优化版） ==========
class SubQueryTool:
    def __init__(self, root):
        self.result_frame = None
        self.root = root
        self.root.title("子查询数据查询工具（修复版）")
        self.root.geometry("1600x800")

        # 初始化变量
        self.db_connector = None
        self.cte_dict = {}
        self.result_df = None
        self.total_count = None

        # 新增：查询模式（默认精确查询）
        self.search_mode = tk.StringVar(value="exact")

        # 新增：当前选中单元格内容
        self.selected_cell_value = ""

        self.original_sql = ""  # 新增：保存执行子查询的原始SQL（无LIMIT）
        self.query_limit = 1000  # 新增：保存查询行数限制

        # 新增：历史连接变量
        self.history_dict = {}
        # 新增：筛选条件存储
        self.filter_conditions = {}

        # 1. 数据库连接配置区域
        self.create_db_config_area()

        # 2. SQL输入区域
        self.create_sql_input_area()

        # 3. 子查询选择区域
        self.create_cte_select_area()

        # 4. 结果展示区域
        self.create_result_area()

        # 新增：加载历史连接到下拉框
        self.load_history_to_combobox()

    # 数据库配置区域
    def create_db_config_area(self):
        frame = ttk.LabelFrame(self.root, text="数据库连接配置（先测试MySQL）")
        frame.pack(fill="x", padx=10, pady=5)

        # 简化配置项，先聚焦MySQL
        ttk.Label(frame, text="数据库类型：").grid(row=0, column=0, padx=5, pady=5)
        self.db_type = ttk.Combobox(frame, values=["MySQL"], width=10)  # 先屏蔽Hive
        self.db_type.grid(row=0, column=1, padx=5, pady=5)
        self.db_type.current(0)

        ttk.Label(frame, text="主机：").grid(row=0, column=2, padx=5, pady=5)
        self.host = ttk.Entry(frame, width=20)
        self.host.grid(row=0, column=3, padx=5, pady=5)
        self.host.insert(0, "127.0.0.1")

        ttk.Label(frame, text="端口：").grid(row=0, column=4, padx=5, pady=5)
        self.port = ttk.Entry(frame, width=10)
        self.port.grid(row=0, column=5, padx=5, pady=5)
        self.port.insert(0, "3306")

        ttk.Label(frame, text="用户名：").grid(row=0, column=6, padx=5, pady=5)
        self.user = ttk.Entry(frame, width=15)
        self.user.grid(row=0, column=7, padx=5, pady=5)
        self.user.insert(0, "root")

        ttk.Label(frame, text="密码：").grid(row=0, column=8, padx=5, pady=5)
        self.password = ttk.Entry(frame, show="*", width=15)
        self.password.grid(row=0, column=9, padx=5, pady=5)

        ttk.Label(frame, text="数据库：").grid(row=0, column=10, padx=5, pady=5)
        self.database = ttk.Entry(frame, width=15)
        self.database.grid(row=0, column=11, padx=5, pady=5)
        self.database.insert(0, "test")  # 默认测试库

        self.connect_btn = ttk.Button(frame, text="连接数据库", command=self.connect_db)
        self.connect_btn.grid(row=0, column=12, padx=10, pady=5)

        # 新增：快速链接下拉框
        ttk.Label(frame, text="快速链接：").grid(row=0, column=13, padx=5, pady=5)
        self.quick_link = ttk.Combobox(frame, width=30, state="readonly")
        self.quick_link.grid(row=0, column=14, padx=5, pady=5)
        self.quick_link.bind("<<ComboboxSelected>>", self.on_quick_link_selected)

    # 新增：加载历史连接到下拉框
    def load_history_to_combobox(self):
        self.history_dict = DBHistoryManager.load_history()

        url_list = list(self.history_dict.keys())
        self.quick_link['values'] = url_list

        if url_list:
            self.quick_link.current(0)

    # 新增：快速链接选择事件（自动填充表单）
    def on_quick_link_selected(self, event):
        selected_url = self.quick_link.get()


        if not selected_url or selected_url not in self.history_dict:
            return

        config = DBHistoryManager.parse_url(selected_url)
        if not config:
            return

        # 清空并填充输入框
        self.host.delete(0, tk.END)
        self.port.delete(0, tk.END)
        self.user.delete(0, tk.END)
        self.password.delete(0, tk.END)
        self.database.delete(0, tk.END)

        self.db_type.set(config.get("db_type", "MySQL"))
        self.host.insert(0, config.get("host", ""))
        self.port.insert(0, config.get("port", ""))
        self.user.insert(0, config.get("user", ""))
        self.password.insert(0, config.get("password", ""))
        self.database.insert(0, config.get("database", ""))

    # SQL输入区域
    def create_sql_input_area(self):
        frame = ttk.LabelFrame(self.root, text="SQL语句输入（示例含WITH子句）")
        frame.pack(fill="both", expand=True, padx=10, pady=5)

        # 示例SQL（方便测试）
        sample_sql = """with xunyuan_chenggong as (select distinct fl.order_no \n
                                                   from xswc.w_fulfill_order_log fl \n
                                                   where operation_type in ('SN校验-pdd-等待回调', 'SN校验-成功', '粗寻源成功新建订单')), \n
                             xinggui as (select fo.original_order_no, \n
                                                fo.external_order_no, \n
                                                coalesce(fl.order_no is not null, 0) is_xunyuan_succeed \n
                                         from xswc.w_fulfill_order fo \n
                                                  left join xunyuan_chenggong fl on fo.order_no = fl.order_no \n
                                         where fo.deleted = 0)
                        select * \n
                        from xinggui"""

        self.sql_text = scrolledtext.ScrolledText(frame, width=150, height=10)
        self.sql_text.pack(fill="both", expand=True, padx=5, pady=5)
        self.sql_text.insert("1.0", sample_sql)  # 自动填充示例SQL

        self.parse_btn = ttk.Button(frame, text="解析子查询", command=self.parse_sql)
        self.parse_btn.pack(side="right", padx=5, pady=5)
    #"""清空所有筛选条件并恢复显示全部数据"""
    def clear_filter_conditions(self):
        if not hasattr(self, 'filter_conditions') or not self.filter_conditions:
             return
        # 清空所有筛选输入框
        for col, entry in self.filter_conditions.items():
            entry.delete(0, tk.END)
         # 重新查询
        self.execute_cte()

    # 子查询选择区域
    def create_cte_select_area(self):
        frame = ttk.LabelFrame(self.root, text="子查询列表")
        frame.pack(fill="x", padx=10, pady=5)

        self.cte_listbox = tk.Listbox(frame, width=50, height=3)
        self.cte_listbox.pack(side="left", padx=5, pady=5)

        self.execute_btn = ttk.Button(frame, text="执行选中子查询", command=self.execute_cte)
        self.execute_btn.pack(side="left", padx=10, pady=5)

        self.export_btn = ttk.Button(frame, text="导出结果", command=self.export_result)
        self.export_btn.pack(side="left", padx=5, pady=5)
        self.export_btn.config(state="disabled")

        self.clear_filter_btn = ttk.Button(frame, text="清空筛选条件", command=self.clear_filter_conditions)
        self.clear_filter_btn.pack(side="left", padx=5, pady=5)

      # 新增：查询模式单选按钮
        ttk.Label(frame, text="查询方式：").pack(side="left", padx=(20, 5))

        self.exact_radio = ttk.Radiobutton(
            frame,
            text = "精确查询",
            variable = self.search_mode,
            value = "exact"
        )
        self.exact_radio.pack(side="left")

        self.fuzzy_radio = ttk.Radiobutton(
            frame,
            text = "模糊查询",
            variable = self.search_mode,
            value = "fuzzy"
        )
        self.fuzzy_radio.pack(side="left", padx=(5, 0))

    # 结果展示区域
    def create_result_area(self):

        # 创建 LabelFrame 并保存为实例变量
        self.result_frame = ttk.LabelFrame(
            self.root,
            text=f"查询结果: 共查询到记录数：{self.total_count}条,（最多展示1000行）"
        )
        self.result_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # # 滚动条 表格
        # self.result_tree = ttk.Treeview(self.result_frame)
        # scroll_y = ttk.Scrollbar(self.result_frame, orient="vertical", command=self.result_tree.yview)
        # scroll_x = ttk.Scrollbar(self.result_frame, orient="horizontal", command=self.result_tree.xview)
        # self.result_tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        # 新增：专门容器承载 Treeview + Scrollbar
        tree_container = ttk.Frame(self.result_frame)
        tree_container.pack(fill="both", expand=True)


        # 新增：筛选器容器
        self.filter_frame = ttk.Frame(self.result_frame)
        self.filter_frame.pack(side="top", fill="x", padx=2, pady=2)


        # 创建 Treeview
        self.result_tree = ttk.Treeview(tree_container, show="headings")
        # 创建滚动条
        scroll_y = ttk.Scrollbar(tree_container, orient="vertical", command=self.result_tree.yview)
        scroll_x = ttk.Scrollbar(tree_container, orient="horizontal", command=self.result_tree.xview)

        # 绑定滚动条
        self.result_tree.configure(
            yscrollcommand = scroll_y.set,
            xscrollcommand = scroll_x.set
        )

        # 正确布局
        self.result_tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")

        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        # 绑定单元格点击事件
        self.result_tree.bind("<ButtonRelease-1>", self.on_tree_click)

        # 绑定 Ctrl+C 复制
        self.result_tree.bind("<Control-c>", self.copy_selected_cell)

        # 添加右键菜单
        self.tree_menu = tk.Menu(self.root, tearoff=0)
        self.tree_menu.add_command(label="复制", command=self.copy_selected_cell)
        self.result_tree.bind("<Button-3>", self.show_tree_menu)


    # 展示结果（新增筛选器功能）
    def create_filter_widgets(self, columns):
        """为每个列创建筛选控件"""
        # 清空原有筛选控件
        for widget in self.filter_frame.winfo_children():
            widget.destroy()

        # 重置筛选条件
        self.filter_conditions = {col: "" for col in columns}

        # 为每个列创建筛选输入框（带漏斗图标）

        # for idx, col in enumerate(columns):
        #     # 列标题 + 漏斗图标
        #     label = ttk.Label(self.filter_frame, text=f"{col} 📊", font=("Arial", 9, "bold"))
        #     label.grid(row=0, column=idx * 2, padx=2, pady=2, sticky="nsew")
        #
        #     # 筛选输入框
        #     filter_entry = ttk.Entry(self.filter_frame, width=15)
        #     filter_entry.grid(row=0, column=idx * 2 + 1, padx=2, pady=2, sticky="nsew")

        # 优化：筛选控件自动换行显示
        # 每行最多显示的列数（可根据实际需要调整）
        cols_per_row = 6
        row_idx = 0
        col_idx = 0
        for col in columns:
            # 列标题 + 漏斗图标
            label = ttk.Label(self.filter_frame, text=f"{col} 📊", font=("Arial", 9, "bold"))
            label.grid(row=row_idx, column=col_idx, padx=2, pady=2, sticky="nsew")
            col_idx += 1
            # 筛选输入框
            filter_entry = ttk.Entry(self.filter_frame, width=15)
            filter_entry.grid(row=row_idx, column=col_idx, padx=2, pady=2, sticky="nsew")
            col_idx += 1
            if col_idx >= cols_per_row * 2:
                col_idx = 0
                row_idx += 1

            # 绑定输入事件（实时筛选）
            filter_entry.bind("<KeyRelease>", lambda e, c=col, entry=filter_entry: self.on_filter_input(c, entry))

            # 存储输入框引用
            self.filter_conditions[col] = filter_entry
    def on_filter_input(self, column, entry):

        if self.result_df is None or self.result_df.empty:
            return

        # 获取所有筛选条件
        filter_vals = {}

        for col, entry in self.filter_conditions.items():
            val = entry.get().strip()
            if val:
                filter_vals[col] = val

        # 核心修改：不再内存过滤，而是拼接SQL条件重新查询数据库
        if not self.original_sql or self.db_connector is None:
            messagebox.showwarning("警告", "无有效原始查询SQL，无法执行数据库端筛选")
            return
        # 拼接WHERE条件
        where_conditions = []
        for col, val in filter_vals.items():
            # 构造模糊查询条件（兼容字符串/数字，防止SQL注入，这里简单处理，生产环境需用参数化）
            # where_conditions.append(f"`{col}` LIKE '%{val}%'")

            # 根据查询模式构造SQL
            if self.search_mode.get() == "fuzzy":
                where_conditions.append(f"`{col}` LIKE '%{val}%'")
            else:
                 where_conditions.append(f"`{col}` = '{val}'")


        # 重构SQL：原始SQL + WHERE条件
        filter_sql = self.original_sql
        base_sql = f"SELECT * FROM ({filter_sql}) as table_name WHERE 1=1"
        # 如果where_conditions不为空，则添加条件
        if where_conditions:
            # 确保每个条件都用AND连接，并去除末尾多余的AND
            conditions_str = " AND ".join(where_conditions)
            full_sql = f"{base_sql} AND {conditions_str}"
        else:
            full_sql = base_sql
        # 执行数据库端筛选
        filtered_df = self.db_connector.execute_sql(full_sql,disable_limit= True)
        if filtered_df is None or filtered_df.empty:
            messagebox.showinfo("提示", "无匹配筛选结果")
            self.show_filtered_result(pd.DataFrame())
            return

        self.total_count = self.db_connector.get_total_count(full_sql)

        # 动态更新结果区域标题
        self.update_result_title()

        # 重新展示筛选后的结果
        self.show_filtered_result(filtered_df)

    def show_filtered_result(self, df):
        # 清空表格
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        # 插入筛选后的数据
        for _, row in df.iterrows():
            values = [str(val) for val in row.values]
            self.result_tree.insert("", "end", values=values)
            # self.result_tree.insert("", tk.END, values=list(row))

    # 更新结果区域标题
    def update_result_title(self):
        self.result_frame.configure(
            text=f"查询结果: 共查询到记录数：{self.total_count}条,（最多展示1000行）"
        )

    # 连接数据库
    def connect_db(self):
        # 清空旧连接
        if self.db_connector:
            self.db_connector.close()

        # 获取配置
        db_type = self.db_type.get()
        host = self.host.get()
        port = self.port.get()
        user = self.user.get()
        password = self.password.get()
        database = self.database.get()

        # 空值校验
        if not host or not port or not user:
            messagebox.showwarning("警告", "主机/端口/用户名不能为空！")
            return

        self.db_connector = DBConnector(db_type, host, port, user, password, database)
        if self.db_connector.connect():
            messagebox.showinfo("成功", "数据库连接成功！")

            # 新增：保存连接信息到历史文件
            config = {
                    "db_type": db_type,
                    "host": host,
                    "port": port,
                    "user": user,
                    "password": password,
                    "database": database
            }
            DBHistoryManager.save_history(config)

            # 新增：刷新快速链接下拉框
            self.load_history_to_combobox()

    # 解析SQL
    def parse_sql(self):
        sql = self.sql_text.get("1.0", tk.END).strip()
        if not sql:
            messagebox.showwarning("警告", "请输入SQL语句！")
            return
        cte_sql = SQLParser.parse_cte_sql(sql)
        executable_cte_sql = SQLParser.build_executable_cte_sql(cte_sql)
        self.cte_dict = executable_cte_sql
        if not self.cte_dict:
            messagebox.showinfo("提示", "未解析到WITH子句中的子查询！")
            return

        # 刷新列表
        self.cte_listbox.delete(0, tk.END)
        for cte_name in self.cte_dict.keys():
            self.cte_listbox.insert(tk.END, cte_name)
        messagebox.showinfo("成功", f"解析到{len(self.cte_dict)}个子查询：{', '.join(self.cte_dict.keys())}")

    # 执行子查询
    def execute_cte(self):
        if not self.db_connector:
            messagebox.showwarning("警告", "请先连接数据库！")
            return

        selected_idx = self.cte_listbox.curselection()
        if not selected_idx:
            messagebox.showwarning("警告", "请选择要执行的子查询！")
            return

        cte_name = self.cte_listbox.get(selected_idx[0])
        cte_sql = self.cte_dict[cte_name]

        # 保存原始SQL
        self.original_sql = cte_sql.strip()

        # 执行
        self.result_df = self.db_connector.execute_sql(cte_sql, limit_rows=self.query_limit, disable_limit= True)

        if self.result_df is None or self.result_df.empty:
            messagebox.showinfo("提示", f"子查询{cte_name}执行完成，无数据返回！")
            return


        self.total_count = self.db_connector.get_total_count(cte_sql)

        # 动态更新结果区域标题
        self.update_result_title()

        # 展示结果
        self.show_result()
        self.export_btn.config(state="normal")
        messagebox.showinfo("成功", f"子查询{cte_name}执行完成，共{self.total_count}条数据！")

    # 展示结果
    def show_result(self):
        # 清空表格
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        # 设置列
        columns = list(self.result_df.columns)
        self.result_tree["columns"] = columns
        self.result_tree["show"] = "headings"
        DEFAULT_COLUMN_WIDTH = 150  # 所有列默认固定宽度
        for col in columns:
            self.result_tree.heading(col, text=col, anchor='w')  # 表头居左
            self.result_tree.column(col, width=120, anchor='w')  # 单元格数据居左
            col_width = DEFAULT_COLUMN_WIDTH
            self.result_tree.column(col, width=col_width, stretch=False)


        # 插入数据
        for _, row in self.result_df.iterrows():
            self.result_tree.insert("", tk.END, values=list(row))
        # 新增：创建筛选控件
        self.create_filter_widgets(columns)

    # 导出结果
    def export_result(self):
        if self.result_df is None or self.result_df.empty:
            messagebox.showwarning("警告", "暂无结果可导出！")
            return

        if not self.db_connector or not self.original_sql:
            messagebox.showwarning("警告", "无有效查询SQL，无法导出！")
            return

        # 获取所有筛选条件
        filter_vals = {}

        for col, entry in self.filter_conditions.items():
            val = entry.get().strip()
            if val:
                filter_vals[col] = val

        # 构造完整SQL（不加LIMIT）
        base_sql = f"SELECT * FROM ({self.original_sql}) as table_name WHERE 1=1"
        where_conditions = []
        for col, val in filter_vals.items():
            # where_conditions.append(f"`{col}` LIKE '%{val}%'")
            if self.search_mode.get() == "fuzzy":
                where_conditions.append(f"`{col}` LIKE '%{val}%'")
            else:
                where_conditions.append(f"`{col}` = '{val}'")



        if where_conditions:
            conditions_str = " AND ".join(where_conditions)
            full_sql = f"{base_sql} AND {conditions_str}"
        else:
            full_sql = base_sql
        # 重新查询数据库（不限制行数）
        try:
            export_df = self.db_connector.execute_sql(full_sql, limit_rows=999999999, disable_limit=False)
            if export_df is None or export_df.empty:
                messagebox.showwarning("提示", "无数据可导出！")
                return
        except Exception as e:
            messagebox.showerror("错误", f"导出查询失败：{str(e)}")
            return







        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel文件", "*.xlsx"), ("CSV文件", "*.csv")]
        )
        if not file_path:
            return

        try:
            if file_path.endswith(".xlsx"):
                export_df.to_excel(file_path, index=False, engine="openpyxl")
            else:
                export_df.to_csv(file_path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("成功", f"结果已导出到：{file_path}")
        except Exception as e:
            messagebox.showerror("失败", f"导出失败：{str(e)}")

  # ==============================
  # 单元格复制功能
  # ==============================
    def on_tree_click(self, event):
      """记录当前点击的单元格内容"""
      region = self.result_tree.identify("region", event.x, event.y)
      if region != "cell":
          return

      row_id = self.result_tree.identify_row(event.y)
      col_id = self.result_tree.identify_column(event.x)

      if not row_id or not col_id:
          return

      col_index = int(col_id.replace("#", "")) - 1

      item = self.result_tree.item(row_id)
      values = item.get("values", [])

      if 0 <= col_index < len(values):
          self.selected_cell_value = str(values[col_index])


    def copy_selected_cell(self, event=None):
      """复制当前选中单元格内容到剪贴板"""
      if not self.selected_cell_value:
          return

      self.root.clipboard_clear()
      self.root.clipboard_append(self.selected_cell_value)
      self.root.update()  # 保证剪贴板更新


    def show_tree_menu(self, event):
      """右键菜单"""
      try:
          self.tree_menu.tk_popup(event.x_root, event.y_root)
      finally:
          self.tree_menu.grab_release()


# ========== 程序入口 ==========
if __name__ == "__main__":
    # 适配高分屏（Windows）
    try:
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(1)
    except:
        pass

    root = tk.Tk()
    app = SubQueryTool(root)
    root.mainloop()