import concurrent.futures
import csv
import functools
import os
import os.path
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Generator

import matplotlib.pyplot as plt
import requests


class TickersDownloader:
    def __init__(self, ticker_names_file: str):
        self.ticker_names_file = ticker_names_file
        self.tickers_data_folder = 'tickers_data'
        self.url_yahoo = 'https://query1.finance.yahoo.com/v8/finance/chart/'
        self.data_queue = queue.Queue()


    def _get_ticker(self) -> Generator[str, None, None]:
        with open(self.ticker_names_file, 'r', encoding='utf-8') as ticker_file:
            for line in ticker_file:
                ticker = line.strip()
                yield ticker


    def _get_history_data(self, ticker_name: str, start_date: datetime, end_date: datetime = datetime.utcnow(), interval: str = '1d') -> dict:
        params = {'period1': str(int(start_date.replace(tzinfo=timezone.utc).timestamp())),
                  'period2': str(int(end_date.timestamp())),
                  'interval': interval,
                  'includeAdjustedClose': 'true'}
        headers = {'User-Agent': 'Mozilla/5.0'}
        url = self.url_yahoo + ticker_name

        try:
            response = requests.get(url, params=params, headers=headers)
        except Exception as err:
            raise Exception(f'Ошибка при загрузке данных для {ticker_name}: {err}.')

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Запрос для загрузки данных для {ticker_name} отклонен сервером.')


    def _format_data(self, json_data: dict) -> list[dict]:
        ticker_symbol = json_data['chart']['result'][0]['meta']['symbol']
        ticker_adjclose = json_data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
        ticker_timestamp = json_data['chart']['result'][0]['timestamp']
        data = [{'symbol': ticker_symbol, 'timestamp': timestamp, 'adjclose': f'{adjclose:02}'} for adjclose, timestamp in zip(ticker_adjclose, ticker_timestamp)]
        return data


    def _save_data_to_csv(self) -> None:
        with open('ticker_data.csv', 'w', encoding='utf-8', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=['symbol', 'timestamp', 'adjclose'])
            csv_writer.writeheader()
            while True:
                data = self.data_queue.get()
                print('Из очереди взяты данные')
                csv_writer.writerows(self._format_data(data))
                self.data_queue.task_done()


    @staticmethod
    def _put_data_to_queue(future: concurrent.futures.Future, queue: queue.Queue) -> None:
        queue.put(future.result())
        print('В очередь добавлены данные.')


    @staticmethod
    def _load_data_to_csv(future: concurrent.futures.Future) -> None:
        json_data = future.result()
        ticker_symbol = json_data['chart']['result'][0]['meta']['symbol']
        ticker_adjclose = json_data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
        ticker_timestamp = json_data['chart']['result'][0]['timestamp']
        data = [{'timestamp': timestamp, 'adjclose': f'{adjclose:0.2f}'} for adjclose, timestamp in zip(ticker_adjclose, ticker_timestamp)]

        os.makedirs('tickers_data', exist_ok=True)
        with open(f'tickers_data/{ticker_symbol}.csv', 'w', encoding='utf-8', newline='') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=['timestamp', 'adjclose'])
            csv_writer.writeheader()
            csv_writer.writerows(data)


    @staticmethod
    def sort_csv_file_by_time(csv_file_name: str) -> None:
        with open(csv_file_name, 'r', encoding='utf-8') as source:
            csv_reader = csv.reader(source)
            header = csv_reader.__next__()
            data = [line for line in csv_reader]

        data.sort(key=lambda line: int(line[1]))

        with open(f'{csv_file_name[:-4]}_sorted.csv', 'w', encoding='utf-8', newline='') as dest:
            csv_writer = csv.writer(dest)
            csv_writer.writerow(header)
            csv_writer.writerows(data)


    def download_data(self, start_date: datetime, end_date: datetime, interval) -> None:
        threading.Thread(target=self._save_data_to_csv, daemon=True).start()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for ticker in self._get_ticker():
                future = executor.submit(self._get_history_data, ticker, start_date, end_date, interval)
                future.add_done_callback(functools.partial(self._put_data_to_queue, queue=self.data_queue))

        self.data_queue.join()


    def download_data_sep_files(self, start_date: datetime, end_date: datetime, interval) -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for ticker in self._get_ticker():
                future = executor.submit(self._get_history_data, ticker, start_date, end_date, interval)
                future.add_done_callback(self._load_data_to_csv)


class TickerHandler:
    def __init__(self, tickers_data_folder: str = 'tickers_data'):
        self.tickers_folder = tickers_data_folder
        self.semaphore = threading.Semaphore(5)
        self.lock = threading.Lock()
        self.data = {}


    def _get_ticker_filename(self) -> Generator[str, None, None]:
        for file in os.listdir(self.tickers_folder):
            if file.endswith('.csv'):
                yield file


    def _process_ticker_file(self, filename: str) -> None:
        with self.semaphore:
            with open(os.path.join(self.tickers_folder, filename), 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                csv_reader.__next__()
                timestamp, start_price = csv_reader.__next__()
                start_price = float(start_price)

                data = [[int(timestamp), start_price, 100]]
                for timestamp, price in csv_reader:
                    data.append([int(timestamp), float(price), round(100 * float(price) / start_price, 2)])

            with self.lock:
                self.data[filename[:-4]] = data

            print(f'Данные для тикера {filename[:-4]} обработаны')


    def collect_data(self) -> None:
        threads = [threading.Thread(target=self._process_ticker_file, args=(filename, )) for filename in self._get_ticker_filename()]
        [thread.start() for thread in threads]
        [thread.join() for thread in threads]


    def plot_graphic(self) -> None:
        for ticker, data in self.data.items():
            dts = []
            adj_prices = []
            for dt, _, adj_price in data:
                dts.append(datetime.fromtimestamp(dt))
                adj_prices.append(adj_price)

            plt.plot(dts, adj_prices, label=ticker)

        plt.legend()
        plt.grid(visible=True, which='both')
        plt.xlim(dts[0], dts[-1])
        plt.ylim(0, 1400)
        plt.show()


if __name__ == '__main__':
    start_time = time.perf_counter()

    downloader = TickersDownloader('tickers.txt')
    downloader.download_data(start_date=datetime(2020, 1, 1), end_date=datetime.utcnow(), interval='1wk')
    downloader.sort_csv_file_by_time('ticker_data.csv')

    downloader.download_data_sep_files(start_date=datetime(2020, 1, 1), end_date=datetime.utcnow(), interval='1wk')

    print(time.perf_counter() - start_time)

    start_time = time.perf_counter()
    handler = TickerHandler()
    handler.collect_data()
    handler.plot_graphic()

    print(time.perf_counter() - start_time)