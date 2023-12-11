import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
import os

STOP_SIGNAL_FILE = 'stop_signal.txt'


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        self.run_script2(event)

    def on_deleted(self, event):
        self.run_script2(event)

    def run_script2(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        dir_name = os.path.dirname(file_path)

        # Проверяем, что изменение произошло в папке payments или bets
        if dir_name.endswith('payments') or dir_name.endswith('bets'):
            print(f'File {event.event_type}: {file_path}')
            subprocess.run(['python', 'script2.py'])


if __name__ == "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)

            if os.path.exists(STOP_SIGNAL_FILE):
                print("Stop signal received. Stopping script1.py.")
                observer.stop()
                os.remove(STOP_SIGNAL_FILE)  # Remove the stop signal file
                break
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

    observer.join()
