import multiprocessing
import time

import pytest
from tasks import DataFetchingTask, DataCalculationTask, DataAggregationTask
from api_client import YandexWeatherAPI
from multiprocessing import Queue
from tests.utils import city_st_peterburg, city_uk


def test_datacalc(monkeypatch):
    """
        Помню что одним из главных правил при создании тестов
        является их независимость друг от друга. А тут получается,
        что если делать в разных тестах, то сделать их независимыми
        не получится т.к. общение между процессами происходит
        через очереди. Пришлось в один тест все засунуть...
    """

    ''' Заполняем очередь данными из замоканной функции '''
    def mock_run(*args, **kwargs):
        if args[0].city == 'LONDON': q.put(city_uk)
        if args[0].city == 'SPETERSBURG': q.put(city_st_peterburg)

    num_workers = multiprocessing.cpu_count()
    yw_api = YandexWeatherAPI()
    q = Queue()
    res_q = Queue()
    analyz_q = Queue()

    monkeypatch.setattr("tasks.DataFetchingTask.run", mock_run)

    thread1 = DataFetchingTask('LONDON', yw_api, q)
    thread2 = DataFetchingTask('SPETERSBURG', yw_api, q)
    for thread in [thread1, thread2]:
        thread.start()

    for thread in [thread1, thread2]:
        thread.join()

    time.sleep(0.1) # Очередь иногда не успевала наполниться
    # Check the contents of the queue after all tasks have completed
    assert not q.empty()

    ''' Проверка результатов расчетов по каждому дню. Должна создаться очередь '''
    processes_calc = [DataCalculationTask(q, res_q) for _ in range(num_workers)]
    for process_calc in processes_calc:
        process_calc.start()

    for _ in range(num_workers):
        q.put(None)

    for process_calc in processes_calc:
        process_calc.join()

    time.sleep(0.1)
    assert not res_q.empty()

    ''' Проверка результатов агрегации. Проверка на создание очереди и проверка полученных данных '''
    processes_agg = [DataAggregationTask(res_q, analyz_q) for _ in range(num_workers)]
    for process_agg in processes_agg:
        process_agg.start()

    for _ in range(num_workers):
        res_q.put(None)

    for process_agg in processes_agg:
        process_agg.join()

    time.sleep(0.1)
    assert not analyz_q.empty()

    while not analyz_q.empty():
        item = analyz_q.get()
        if item['City'] == 'England': assert item['TotalAvgTemp'] == 8.27 and item['TotalGoodHours'] == 27
        if item['City'] == 'St. Petersburg': assert item['TotalAvgTemp'] == 8.39 and item['TotalGoodHours'] == 24
