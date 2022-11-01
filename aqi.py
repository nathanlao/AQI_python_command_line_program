import requests
import json
import pandas as pd
import time
import statistics

sample_size = 5


# Debugging purpose
def printJSON(obj):
    # Formatted JSON object
    text = json.dumps(obj, sort_keys=True, indent=4)
    print(text)


def getAPIResponseByMapBound():
    # map bound provided by http://bboxfinder.com/#48.960091,-123.451267,49.380294,-122.012058
    mapBound = "48.960091,-123.451267,49.380294,-122.012058"
    token = 'd2a743077d1475b52b69534c6208e2d5fff7de2c'
    url = f'https://api.waqi.info/map/bounds?token={token}&latlng={mapBound}'

    response = requests.get(url).json()

    rows = response['data']
    data = createDataFrame(rows)

    # Sample number of station data according to sample_size
    data = data.sample(n=sample_size)

    # Debugging purpose:
    # print(data)

    return data


def createDataFrame(rows):
    data = pd.DataFrame(columns=['lat', 'lon', 'station', 'time'])

    lat = []
    lon = []
    station = []
    times = []

    for row in rows:
        lat.append(row['lat'])
        lon.append(row['lon'])
        station.append(row['station']['name'])
        times.append(row['station']['time'])

    data['lat'] = lat
    data['lon'] = lon
    data['station'] = station
    data['time'] = time

    # Only interested in BC stations for now (drop others if any)
    data = data[data["station"].str.contains("British Comlumbia")] # name typo in JSON API

    return data


def getAPIResponseByLatLon(data):

    global pm25_rows

    lat = data['lat']
    lon = data['lon']

    temp = pd.DataFrame(columns=['lat', 'lon', 'station_name'])
    temp['lat'] = lat
    temp['lon'] = lon

    times = []
    pm25 = []
    station = []

    for i in range(len(temp)):
        lat, lon, station_name = temp.iloc[i]
        token = 'd2a743077d1475b52b69534c6208e2d5fff7de2c'
        url = f'https://api.waqi.info/feed/geo:{lat};{lon}/?token={token}'
        response2 = requests.get(url).json()

        # Debugging purpose:
        # printJSON(response2)

        name_rows = response2["data"]['city']['name']
        station.append(name_rows)

        time_rows = response2["data"]['time']['s']
        times.append(time_rows)

        try:
            pm25_rows = response2['data']['iaqi']['pm25']
        except KeyError:
            pass

        pm25.append(pm25_rows['v'])

    PM25_df = pd.DataFrame(columns=['station_name', 'pm25', 'time'])
    PM25_df['station_name'] = station
    PM25_df['time'] = times
    PM25_df['pm25'] = pm25

    # Calculate average of pm25 column
    average_of_PM25 = PM25_df["pm25"].mean()

    return average_of_PM25, PM25_df


def main():
    average_of_PM25 = []  # Store a list of average of PM2.5 from n samples

    sampling_period = 60*5  # default == 5 in minutes (also as a countdown)
    sampling_rate = sample_size // sampling_period

    next_time = time.time()
    print("PM2.5 sampled value for each station: ")
    while True:
        start_time = time.time()

        # get stations by Map bound
        response_dataFrame = getAPIResponseByMapBound()

        # Get PM25 by a pair of latitude/longitude
        avg_PM25, dataFrame = getAPIResponseByLatLon(response_dataFrame)
        print(dataFrame)
        average_of_PM25.append(avg_PM25)

        # suspends (waits) execution of the current thread (send request to API every n minutes
        next_time += sampling_period // sample_size
        sleep_time = next_time - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)

        end_time = time.time()

        # Time period ends
        sampling_period -= (end_time-start_time)
        if sampling_period < sampling_rate:
            break

    print(average_of_PM25)
    print("\n")
    print("Overall PM2.5 average of all stations over n (default = 5) "
          "minutes of sampling: {:.2f}".format(statistics.mean(average_of_PM25)))


if __name__ == '__main__':
    main()
