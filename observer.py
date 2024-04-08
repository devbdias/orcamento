from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, data_path):
        super(FileChangeHandler, self).__init__()
        self.data_path = data_path

    def on_any_event(self, event):
        if event.event_type == 'created' or event.event_type == 'deleted':
            files = os.listdir(self.data_path)
            num_files = len(files)
            print(f"Total de arquivos: {num_files}")

def monitorar_diretorio(data_path):
    event_handler = FileChangeHandler(data_path)
    observer = Observer()
    observer.schedule(event_handler, path=data_path, recursive=False)
    observer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    data_path = os.path.join("src", "data", "raw")
    monitorar_diretorio(data_path)
