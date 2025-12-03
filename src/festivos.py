import holidays
from datetime import date, timedelta

if __name__ == "__main__":

    es_holidays = holidays.country_holidays("ES")

    d1 = date(2022, 1, 1)
    d2 = date(2023, 1, 1)

    delta = d2 - d1 

    findes_o_festivos = 'fecha, es_festivo, es_fin_de_semana\n'

    for i in range(delta.days + 1):
        day = d1 + timedelta(days=i)
        if day in es_holidays or day.weekday() == 5 or day.weekday() == 6:
            findes_o_festivos += f'{day}, {1 if day in es_holidays else 0}, {1 if day.weekday() > 4 else 1}\n'
    
    print(findes_o_festivos)

    with open('data/festivos.csv', 'w') as f:
        f.write(findes_o_festivos)
        #print(day, day in es_holidays)

    #es_holidays = holidays.country_holidays("ES")
    #print(es_holidays.get('2024-01-01'))
