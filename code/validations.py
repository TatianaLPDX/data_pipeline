import datetime

test = {"EVENT_NO_TRIP": "168733674", "EVENT_NO_STOP": "168733676", "OPD_DATE": "24-SEP-20", 
"VEHICLE_ID": "4036", "METERS": "75843", "ACT_TIME": "34217", "VELOCITY": "5", "DIRECTION": "80", 
"RADIO_QUALITY": "", "GPS_LONGITUDE": "-122.661302", "GPS_LATITUDE": "45.630505", "GPS_SATELLITES": "9", 
"GPS_HDOP": "0.8", "SCHEDULE_DEVIATION": "99"}

class Validations:
    def validateVelocity(self, data):
        key = 'VELOCITY'
        if key not in data:
            print("Velocity doesn't exist in data!")
            return False

        velocity = data[key]
        try:
            velocity = int(data[key])
        except ValueError:
            print( "The record has an abnormal velocity of {}".format(velocity))
            return False

        if (0 <= velocity <= 50) is False:
            print( "The record has an abnormal velocity of {}".format(velocity))
        return float(velocity * 2.23694)

    def validateLatitude(self, data):
        key = 'GPS_LATITUDE'
        if key not in data:
            print("Latitude doesn't exist in data!")
            return False

        latitude = data[key]
        try:
            latitude = float(data[key])
        except ValueError:
            print("The record has an abnormal latitude of {}".format(latitude))
            return False

        latitudeOfPortland = 45.5051
        errorRange = 10

        if (latitudeOfPortland - errorRange < latitude < latitudeOfPortland + errorRange) is False:
            print("The record has an abnormal latitude of {}".format(latitude))
            return False
        return latitude

    def validateLongitude(self, data):
        key = 'GPS_LONGITUDE'
        if key not in data:
            print("Longitude doesn't exist in data!")
            return False

        longitude = data[key]
        try:
            longitude = float(data[key])
        except ValueError:
            print("The record has an abnormal longitude of {}".format(longitude))
            return False

        longitudeOfPortland = -122.6750
        errorRange = 10
        if (longitudeOfPortland - errorRange < longitude < longitudeOfPortland + errorRange) is False:
            print("The record has an abnormal longitude of {}".format(longitude))
            return False
        return longitude

    def validateOperationDay(self, data):
        key = "OPD_DATE"
        if key not in data:
            print("Operation day doesn't exist in data!")
            return

        date = data[key].split("-")
        try:
            day = int(date[0])
            year = int(date[2])
        except ValueError:
            print("The record has an invalid date")
            return False
        months = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
        if date[1] not in months:
            print("The record has an invalid month")
            return False

        month = months[date[1]]
        newDate = None
        try:
            newDate = datetime.datetime(2000 + year, month, day, 0, 0, 0)
        except ValueError:
            print("The record has an invalid date: day - {}, month - {}, year - {}".format(day, month, year))
            return False
        return newDate

    def validateDirection(self, data):
        key = "DIRECTION"
        if key not in data:
            print("DIRECTION doesn't exist in data!")
            return False

        direction = data[key]
        try:
            direction = int(data[key])
        except ValueError:
            print("The record has an abnormal direction of {}!".format(direction))
            return False
        if (0 <= direction <= 360) is False:
            print("The record has an abnormal direction of {}!".format(direction))
            return False
        return direction

    def validateMeters(self, data):
        key = "METERS"
        if key not in data:
            print("METERS doesn't exist in data!")
            return False

        meters = data[key]
        if meters.isnumeric() is False:
            print("The record has an abornal meters of {}!".format(meters))
            return False
        return meters

    def validateRadioQuality(self, data):
        key = "RADIO_QUALITY"
        if key not in data:
            print("Radio quality doesn't exist in data!")
            return False

        radio_quality = data[key]
        if radio_quality.strip() != "":
            print("The record has an abornal radio quality of {}!".format(radio_quality))
            return False
        return True

    def validateHDOP(self, data):
        key = "GPS_HDOP"
        if key not in data:
            print("HDOP doesn't exist in data!")
            return False

        hdop = data[key]
        try:
            hdop = float(data[key])
        except ValueError:
            print("The record has a poor HDOP value of {}!".format(hdop))
            return False
        if hdop >= 20:
            print("The record has a poor HDOP value of {}!".format(hdop))
            return False
        return hdop

    def validateSatellites(self, data):
        key = "GPS_SATELLITES"
        if key not in data:
            print("Satellites doesn't exist in data!")
            return False

        satellites = data[key]
        try:
            satellites = int(data[key])
        except ValueError:
            print("The record has an abnormal amount of satellites of {}!".format(satellites))
            return False
        if (0 < satellites < 50) is False:
            print("The record has an abnormal amount of satellites of {}!".format(satellites))
            return False
        return satellites

    def validateScheduleDeviation(self, data):
        key = "SCHEDULE_DEVIATION"
        if key not in data:
            print("Schedule Deviation doesn't exist in data!")
            return False

        sd = data[key]
        try:
            sd = float(sd)
        except ValueError:
            print("The record has an abnormal schedule deviation of {}!".format(sd))
            return False
        return sd

    def validateActTime(self, data):
        key = "ACT_TIME"
        if key not in data:
            print("Actual time doesn't exist in data!")
            return False

        act_time = data[key]
        try:
            act_time = int(data[key])
        except ValueError:
            print("The record has an abnormal actual time of {}!".format(act_time))
            return False

        if 0 < act_time < 86400 is False:
            print("The record has an abnormal actual time of {}!".format(act_time))
            return False
        return act_time

    def validateVehicleID(self, data):
        key = "VEHICLE_ID"
        if key not in data:
            print("Vehicle ID doesn't exist in data!")
            return False

        id = data[key]
        try:
            id = int(data[key])
        except ValueError:
            print("The record has an abnormal vehicle ID of {}!".format(id))
            return False
        return id

    def validateTripID(self, data):
        key = "EVENT_NO_TRIP"
        if key not in data:
            print("Trip ID doesn't exist in data!")
            return False

        id = data[key]
        try:
            id = int(data[key])
        except ValueError:
            print("The record has an abnormal trip ID of {}!".format(id))
            return False
        return id

    def validateStopID(self, data):
        key = "EVENT_NO_STOP"
        if key not in data:
            print("Stop ID doesn't exist in data!")
            return False

        id = data[key]
        try:
            id = int(data[key])
        except ValueError:
            print("The record has an abnormal stop ID of {}!".format(id))
            return False
        return id


v = Validations()

v.validateStopID(test)
v.validateTripID(test)
v.validateVehicleID(test)
v.validateActTime(test)
v.validateScheduleDeviation(test)
v.validateSatellites(test) 
v.validateHDOP(test)
v.validateRadioQuality(test)
v.validateMeters(test)
v.validateDirection(test)
v.validateVelocity(test)
v.validateLatitude(test)
v.validateLongitude(test)
v.validateOperationDay(test)
