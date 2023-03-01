import multiprocessing
import time
from multiprocessing import Queue

from api_client import YandexWeatherAPI
from mylogger import logger as log
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather() -> None:
    """
    Анализ погодных условий по городам
    """
    # Наполняю multiprocessing.Queue из потоков
    q: Queue = Queue()     # Далее достану элементы из процесса DataCalculationTask
    res_q: Queue = Queue()    # Складываю результаты агрегации
    analyz_q: Queue = Queue()  # Складываю результаты данные для анализа
    yw_api = YandexWeatherAPI()
    num_workers = multiprocessing.cpu_count()

    log.info(f'Количество городов на входе - {len(CITIES)}')

    '''
        Процесс расчетов
        Он в бесконечном цикле читает из очереди в которую пишет поток
    '''
    processes_calc = [DataCalculationTask(q, res_q) for _ in range(num_workers)]
    for process_calc in processes_calc:
        process_calc.start()
        log.info(f'Process: DataCalculationTask-{process_calc.pid} started')

    '''
        Создаю процесс агрегации который в бесконечном цикле
        читает из очереди в которую пишут процессы расчета
    '''
    processes_agg = [DataAggregationTask(res_q, analyz_q) for _ in range(num_workers)]
    for process_agg in processes_agg:
        process_agg.start()
        log.info(f'Process: DataAggregationTask-{process_agg.pid} started')

    threads = [DataFetchingTask(city, yw_api, q) for city in CITIES.keys()]
    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    ''' Отправляю None в очередь чтобы выйти из цикла в DataAggregationTask '''
    for _ in range(num_workers):
        res_q.put(None)

    ''' Отправляю None в очередь чтобы выйти из цикла в DataCalculationTask '''
    for _ in range(num_workers):
        q.put(None)

    ''' Ждем завершения всех процессов DataAggregationTask '''
    for process_agg in processes_agg:
        process_agg.join()

    ''' Ждем завершения всех процессов DataCalculationTask '''
    for process_calc in processes_calc:
        process_calc.join()

    '''
        Когда первые 2 процесса завершились, запускаю 1 процесс DataAnalyzing
        который сделает сортировку и положит данные в файл
    '''
    processes_analyze = DataAnalyzingTask(analyz_q)
    processes_analyze.run()
    log.info(f'Process: DataAnalyzingTask - started')


if __name__ == "__main__":
    start = time.time()
    forecast_weather()
    log.info(
        'Done in {:.4} - agregation/analyzing data'.format(time.time() - start)
    )
