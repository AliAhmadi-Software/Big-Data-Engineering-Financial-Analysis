# استفاده از یک تصویر پایه Ubuntu
FROM ubuntu:20.04

# تنظیمات برای جلوگیری از خطاهای locale
ENV DEBIAN_FRONTEND=noninteractive

# نصب پیش‌نیازها
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.7 python3.7-dev python3.7-distutils python3-pip curl && \
    apt-get clean


# نصب pip برای Python 3.7
RUN curl https://bootstrap.pypa.io/pip/3.7/get-pip.py -o get-pip.py && python3.7 get-pip.py

# نصب Jupyter Notebook و کتابخانه‌های مورد نیاز
RUN pip install notebook protobuf==3.20.* tensorflow==1.15 setuptools matplotlib pandas numpy scikit-learn

# ایجاد دایرکتوری کاری
WORKDIR /tf1_notebook

# پورت Jupyter Notebook
EXPOSE 8888

# دستور اجرا
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser"]