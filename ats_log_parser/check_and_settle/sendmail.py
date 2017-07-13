# -*- coding: utf-8 -*-
import logging
import smtplib
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


def send_mail(filename, file_path, mail_account, mail_password, tomail_account):
    sender = mail_account
    receivers = tomail_account  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    # 创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header(mail_account)
    message['To'] = Header(str(tomail_account))
    # 邮件主题
    if dt.datetime.today().strftime("%H:%M:%S") < "15:00:00":
        subject = "{today}夜盘持仓情况".format(today=dt.date.today().strftime("%Y%m%d"))
    else:
        subject = "{today}午盘持仓情况".format(today=dt.date.today().strftime("%Y%m%d"))
    message['Subject'] = Header(subject)

    # 邮件正文内容
    message.attach(MIMEText(''))

    # 构造附件1，传送当前目录下的 test.txt 文件
    att = MIMEText(open(file_path, 'rb').read(), 'base64', 'utf-8')
    att["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att["Content-Disposition"] = 'attachment; filename="%s"' % filename
    message.attach(att)

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect('mail.liangyiinvestment.com')
        smtpObj.login(mail_account, mail_password)
        smtpObj.sendmail(sender, receivers, message.as_string())
        logging.info("邮件发送成功")
    except smtplib.SMTPException:
        logging.info("Error: 无法发送邮件")


if __name__ == '__main__':
    print("----START----")
    file_path = r'E:\HX_python\ats_log_parser\LICENSE'
    filename = 'LICENSE'
    send_mail(filename, file_path, 'hongxing@liangyiinvestment.com', 'Hx142101', ['hong142101@163.com'])
