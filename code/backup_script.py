import os.path
from os import path
from datetime import date
import smtplib, ssl
import backup_pipeline
import pipeline
config_file = "home/tatinal.config.conf"

#checking if main files were saved and pipeline sucessful for today
def run_test():
    today = date.strftime("%b_%d_%Y")
    file_name = today + '.json'
    if not (path.exists("/home/tatiana/sensor_data/sensor_{file_name}") or path.exists("/home/tatiana/stop_data/stop_{file_name}")):
        return False
    else:
        return True

#run backups to save files from failed pipeline day and sed the data to producer/consumer once again
def run_backup():
    today = date.strftime("%b_%d_%Y")
    #save backup files for sensor data
    pipeline.save_sensor_data.py(today)
    pipeline.save_stop_data.py(today)

    #save backup files for stop data
    backup_pipeline.producer_sensor.py(today)
    backup_pipeline.consumer_sensor.py(today)
    backup_pipeline.producer_stop.py(today)
    backup_pipeline.consumer_stop.py(today)

#sending email about failed tests/pipeline
def send_email():
    port = config_file['email_server_port']
    smtp_server = config_file['send_from_server']
    sender_email = config_file['send_from_email']
    receiver_email = config_file['send_to_email']
    password = config_file['email_pw']
    today = date.strftime("%b_%d_%Y")
    message = """\
        Subject: Pipeline Failed Date: {today_date}
        Your data pipeline failed for {today_date}, filed saved into a directory for backups."""

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.starttls(context=context)
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)

def main():

#identify if backup script needs to run
    if not run_test():
        run_backup()
        send_email()

if __name__== "__main__":
   main()