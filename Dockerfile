# 基础镜像：选择官方轻量的 Python 镜像（3.11 为示例，可换成你的项目版本）
FROM python:3.11-bullseye

# 设置工作目录（容器内的目录）
WORKDIR /app

# 复制项目依赖文件到容器
COPY requirements.txt .

# 安装依赖（加 --no-cache-dir 减少镜像体积）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 复制整个项目代码到容器
COPY . .

# 暴露端口（如果项目是 Web 服务，比如 Flask/Django，需指定端口）
EXPOSE 8501

# 启动命令（替换成你的项目启动命令，比如 python app.py）
CMD ["python", "app.py"]
