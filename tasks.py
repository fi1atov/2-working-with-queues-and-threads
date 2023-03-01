import csv
import time
from multiprocessing import Process, Queue
from operator import itemgetter
from statistics import mean
import concurrent.futures
from threading import Thread

from api_client import YandexWeatherAPI
from mylogger import logger as log
from utils import GOOD_WEATHER


class DataFetchingTask(Thread):
    def __init__(
            self,
            city: str,
            ya_weather: YandexWeatherAPI,
            queue: Queue
    ) -> None:
        super().__init__()
        self.city = city
        self.ya_weather = ya_weather
        self.queue = queue

    def run(self) -> None:
        response = self.ya_weather.get_forecasting(self.city)
        log.info(f'Thread {self.name}: Получены данные из API по городу {self.city}')
        self.queue.put(response)
        log.info(f'Thread {self.name}: Данные помещены в очередь для расчета по городу {self.city}')


class DataCalculationTask(Process):
    START_HOUR = 9
    END_HOUR = 19

    def __init__(self, q: Queue, res_q: Queue) -> None:
        super().__init__()
        self.q = q
        self.res_q = res_q

    def run(self) -> None:

        while True:

            item = self.q.get()
            if item is None:  # Если пришел None - тогда прекращаем работу процессов
                break

            city = item["geo_object"]["province"]["name"]
            log.info(f'Process {self.name}: Взял данные из очереди для расчета по городу: {city}')

            city_data_per_day = []

            for day in item["forecasts"]:
                good_hours_counter = 0
                list_temp_per_day = []
                date = day["date"]
                for hour in day["hours"]:
                    if (int(hour["hour"]) >= self.START_HOUR) and (int(hour["hour"]) <= self.END_HOUR):
                        list_temp_per_day.append(int(hour["temp"]))
                    if hour["condition"] in GOOD_WEATHER:
                        good_hours_counter += 1

                avg_temp_per_day = round(mean(list_temp_per_day), 2) if list_temp_per_day != [] else None

                data_for_day = {
                    "Date": date,
                    "AvgTempPerDay": avg_temp_per_day,
                    "CountGoodHours": good_hours_counter
                }
                city_data_per_day.append(data_for_day)

            city_data = {
                "City": city,
                "Data": city_data_per_day
            }

            self.res_q.put(city_data)

            log.info(f'Process {self.name}: Положил рассчитанные данные в очередь агрегации по городу: {city}')


class DataAggregationTask(Process):

    def __init__(self, res_q: Queue, analyz_q: Queue) -> None:
        super().__init__()
        self.res_q = res_q
        self.analyz_q = analyz_q

    def run(self) -> None:

        while True:
            city_data = self.res_q.get()
            if city_data is None:  # Если пришел None - тогда прекращаем работу процессов
                break

            log.info(f'Process {self.name}: Взял данные из очереди для агрегации по городу: {city_data["City"]}')

            total_avg_temp = 0
            total_good_hours = 0
            counter = 0
            for date in city_data["Data"]:
                total_avg_temp += date["AvgTempPerDay"] if date["AvgTempPerDay"] is not None else 0
                total_good_hours += date["CountGoodHours"]
                counter += 1

            city_data["TotalAvgTemp"] = round(total_avg_temp / counter, 2)
            city_data["TotalGoodHours"] = total_good_hours

            self.analyz_q.put(city_data)

            log.info(f'Process {self.name}: Поместил данные в очередь на анализ по городу: {city_data["City"]}')


class DataAnalyzingTask:

    def __init__(self, analyz_queue: Queue) -> None:
        self.analyz_queue = analyz_queue

    @staticmethod
    def write_to_csv(rows, filename):

        """ Транспонировать таблицу оказывается довольно сложным делом... """
        with open(filename, 'w', newline='') as file:

            writer = csv.DictWriter(
                file,
                fieldnames=[
                    'Город',
                    'Дата',
                    'Температура, среднее',
                    'Без осадков, часов',
                    'Среднее',
                    'Рейтинг'
                ]
            )

            writer.writeheader()

            for city_data in rows:
                city = city_data['City']
                total_avg_temp = city_data['TotalAvgTemp']
                rating = city_data['Rating']

                for date_data in city_data['Data']:
                    date = date_data['Date']
                    avg_temp_per_day = date_data['AvgTempPerDay']
                    count_good_hours = date_data['CountGoodHours']

                    writer.writerow({
                        'Город': city,
                        'Дата': date,
                        'Температура, среднее': avg_temp_per_day,
                        'Без осадков, часов': count_good_hours,
                        'Среднее': total_avg_temp,
                        'Рейтинг': rating
                    })

    def run(self) -> None:
        data = []
        file_name = 'result.csv'
        process_name = 'Main'

        log.info(f'Process {process_name}: Собирает данные из очереди для анализа...')
        while not self.analyz_queue.empty():
            city = self.analyz_queue.get()
            data.append(city)
            log.info(f'Process {process_name}: Получаю данные для города: {city["City"]}')

        log.info(f'Process {process_name}: Получил данные для анализа городов. Количество: {len(data)}')
        log.info(f'Process {process_name}: Данные для анализа собраны - начинаю сортировку...')
        new_data = sorted(data, key=itemgetter('TotalAvgTemp', 'TotalGoodHours'), reverse=True)

        for i, item in enumerate(new_data):
            item["Rating"] = i + 1
            item.pop('TotalGoodHours')

        log.info(f'Process {process_name}: Данные отсортированы - начинаю запись в файл...')

        # Lock для захвата csv не нужен - ThreadPoolExecutor сам позаботится об этом
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(self.write_to_csv, new_data, file_name)

        log.info(f'Process {process_name}: Результаты анализа расположены в файле: {file_name}')


if __name__ == '__main__':
    analyz_queue = Queue()

    v1 = {
        'City': 'Abu Dhabi',
        'Data': [
            {'Date': '2022-05-26', 'AvgTempPerDay': 34.82, 'CountGoodHours': 24},
            {'Date': '2022-05-27', 'AvgTempPerDay': 34.45, 'CountGoodHours': 24},
            {'Date': '2022-05-28', 'AvgTempPerDay': 33.82, 'CountGoodHours': 24},
            {'Date': '2022-05-29', 'AvgTempPerDay': 34, 'CountGoodHours': 11},
            {'Date': '2022-05-30', 'AvgTempPerDay': None, 'CountGoodHours': 0}
        ],
        'TotalAvgTemp': 27.42,
        'TotalGoodHours': 83
    }
    v2 = {
        'City': 'Hebei Province',
        'Data': [
            {'Date': '2022-05-26', 'AvgTempPerDay': 31.82, 'CountGoodHours': 24},
            {'Date': '2022-05-27', 'AvgTempPerDay': 32.73, 'CountGoodHours': 24},
            {'Date': '2022-05-28', 'AvgTempPerDay': 33.82, 'CountGoodHours': 24},
            {'Date': '2022-05-29', 'AvgTempPerDay': 28.17, 'CountGoodHours': 15},
            {'Date': '2022-05-30', 'AvgTempPerDay': None, 'CountGoodHours': 0}
        ],
        'TotalAvgTemp': 25.31,
        'TotalGoodHours': 87
    }

    analyz_queue.put(v1)
    analyz_queue.put(v2)

    time.sleep(1)
    ob = DataAnalyzingTask(analyz_queue)
    ob.run()
