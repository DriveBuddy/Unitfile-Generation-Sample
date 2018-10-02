import calendar
from datetime import datetime

from pandas import read_csv

import unitfile_pb2


def str_to_datetime(s):
    [h, m, s, x] = s.split(':')
    x = int(x) * 1000
    return datetime(2018, 1, 1, int(h), int(m), int(s), int(x))


def datetime_unitfile_format(d):
    return '{y:2}{m:2}{d:2}-{h:2}{min:2}{s:2}'.format(
        y=d.year % 100,
        m=d.month,
        d=d.day,
        h=d.hour,
        min=d.minute,
        s=d.second,
    ).replace(' ', '0')


def generate_unitfiles(driver_id, filename):
    data = read_csv(filename, delimiter=';')

    accs = data[['Time_Accel', 'AccelX', 'AccelY', 'AccelZ']]
    accs = accs.drop_duplicates(subset='Time_Accel')

    locs = data[['Time_GPS', 'Latitude_GPS', 'Longitude_GPS', 'Accuracy_GPS', 'Speed_GPS']]
    locs = locs.drop_duplicates(subset='Time_GPS')

    locs['Time_GPS'] = locs['Time_GPS'].apply(str_to_datetime)
    accs['Time_Accel'] = accs['Time_Accel'].apply(str_to_datetime)

    locs = locs.reset_index(drop=True)
    accs = accs.reset_index(drop=True)

    start_time = locs['Time_GPS'][1]
    if accs['Time_Accel'][0] < start_time:
        start_time = accs['Time_Accel'][0]

    end_time = locs['Time_GPS'][locs['Time_GPS'].size - 1]
    if accs['Time_Accel'][accs['Time_Accel'].size - 1] > end_time:
        end_time = accs['Time_Accel'][accs['Time_Accel'].size - 1]

    u = unitfile_pb2.UnitFile()
    u.driver_id = driver_id
    u.start_time = calendar.timegm(start_time.timetuple())
    u.end_time = calendar.timegm(end_time.timetuple())
    u.timezoneoffset = 0

    loc_data = []

    for i in range(1, locs['Time_GPS'].size):

        loc_datum = unitfile_pb2.LocData()
        loc_datum.timestamp = calendar.timegm(locs['Time_GPS'][i].timetuple())
        loc_datum.latitude = locs['Latitude_GPS'][i]
        loc_datum.longitude = locs['Longitude_GPS'][i]
        loc_datum.accuracy = locs['Accuracy_GPS'][i]
        loc_datum.speed = locs['Speed_GPS'][i]
        loc_datum.direction = (locs['Speed_GPS'][i] - locs['Speed_GPS'][i - 1] > 0)
        loc_datum.speed = locs['Speed_GPS'][i]

        acc_part = accs[accs['Time_Accel'] < locs['Time_GPS'][i]][locs['Time_GPS'][i - 1] <= accs['Time_Accel']]
        acc_part = acc_part.reset_index(drop=True)

        acc_data = []

        for j in range(acc_part['Time_Accel'].size):
            acc_datum = unitfile_pb2.AccData()
            acc_datum.x = acc_part['AccelX'][j]
            acc_datum.y = acc_part['AccelY'][j]
            acc_datum.z = acc_part['AccelZ'][j]
            acc_datum.timestamp = calendar.timegm(acc_part['Time_Accel'][j].timetuple())
            acc_data.append(acc_datum)

        loc_datum.acc_data.extend(acc_data)
        loc_data.append(loc_datum)

    u.loc_data.extend(loc_data)

    with open('unitfiles/unit_{id}_{s}_{e}'.format(id=driver_id, s=datetime_unitfile_format(start_time),
                                                   e=datetime_unitfile_format(end_time)), "wb") as f:
        f.write(u.SerializeToString())

    return u


def main():
    generate_unitfiles(95, 'dataset_web/participant_1.csv')


if __name__ == '__main__':
    main()

