import sys
from loguru import logger

scenario = "ALL"    # "ALL" , "FILE_ONLY" , "STDOUTPUT_ONLY"
level = "DEBUG"  # Имена уровней такие же как в стандартном модуле logging, только текстом

logger.remove(None)  # удалить все настройки по умолчанию
if scenario in ("FILE_ONLY", "ALL"):
    logger.add("analyzer.log",   # имя файла
               encoding="utf8",  # кодировку можно задать (только для файла)
               format="{time} | {level: <8} | {name: ^15} | {function: ^15} | {line: >3} | {message}",
               enqueue=True,  # нужно для поддержки асинхронности процессов
               level=level)
if scenario in ("STDOUTPUT_ONLY", "ALL"):
    logger.add(sys.stdout,  # вывод в консоль, только еще цвета настраиваются =)
               format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                      "<level>{level: <8}</level> | "
                      "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
               enqueue=True,  # нужно для поддержки асинхронности процессов
               level=level)
