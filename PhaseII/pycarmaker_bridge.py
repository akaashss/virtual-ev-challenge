# pycarmaker_bridge.py (pp5_final_logic.py)
from pycarmaker import CarMaker, Quantity
import time
import datetime
from kuksa_client.grpc import VSSClient, Datapoint

# ---- CarMaker Connection ----
IP_ADDRESS = "localhost"
PORT = 16660
cm = CarMaker(IP_ADDRESS, PORT)

print("Attempting to connect to CarMaker...")
cm.connect()
print(" Successfully connected to CarMaker.")

# --- Quantities to READ from CarMaker ---
soc_cm = Quantity("PT.BCU.BattHV.SOC", Quantity.FLOAT)
position_cm = Quantity("Car.Distance", Quantity.FLOAT)

# --- Quantities to WRITE to CarMaker ---
# These objects represent the individual booking signals for each station
ch1_book_cm = Quantity("CH1_01.Book", Quantity.INT)
ch2_book_cm = Quantity("CH2_01.Book", Quantity.INT)
ch3_book_cm = Quantity("CH3_01.Book", Quantity.INT)
ch4_book_cm = Quantity("CH4_01.Book", Quantity.INT)
ch5_book_cm = Quantity("CH5_01.Book", Quantity.INT)
c1 = Quantity("CH1_01.Response", Quantity.INT)
c2 = Quantity("CH2_01.Response", Quantity.INT)
c3 = Quantity("CH3_01.Response", Quantity.INT)
c4 = Quantity("CH4_01.Response", Quantity.INT)
c5 = Quantity("CH5_01.Response", Quantity.INT)

# This is the single quantity we will write the final mapped value to
charge_Request = Quantity("Car.Charge_Request", Quantity.FLOAT)


print("Subscribing to CarMaker quantities: PT.BCU.BattHV.SOC, Car.Distance")
cm.subscribe(soc_cm)
cm.subscribe(position_cm)
cm.read(); cm.read()

# --- VSS Signals for Station Status ---
STATION_VSS_SIGNALS = [
    'Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure',
    'Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure',
    'Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure',
    'Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure',
    'Vehicle.Chassis.Brake.PedalPosition',
]

# Mapping from VSS Signal back to the CarMaker Quantity object for that station
VSS_TO_CM_MAP = {
    'Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure': ch1_book_cm,
    'Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure': ch2_book_cm,
    'Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure': ch3_book_cm,
    'Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure': ch4_book_cm,
    'Vehicle.Chassis.Brake.PedalPosition': ch5_book_cm,
}

# Maps the chosen station object to the specific value required by the competition
STATION_TO_CHARGE_VALUE_MAP = {
    ch1_book_cm: 1,   # If CH1 is chosen, write 1.0
    ch2_book_cm: 5,   # If CH2 is chosen, write 5.0
    ch3_book_cm: 8,   # etc.
    ch4_book_cm: 11,
    ch5_book_cm: 14,
}

print("\n--- Starting Real-time Data Bridge (Two-Way) ---")
print("Using standard VSS signals for station status.")
print("Press Ctrl+C to stop the script.")

try:
    with VSSClient(host="127.0.0.1", port=55555) as kuksa:
        print(" Successfully connected to KUKSA databroker.")

        while True:
            # === PART 1: CarMaker to KUKSA ===
            cm.read()
            current_soc = soc_cm.data
            current_position = position_cm.data

            if current_soc is not None and current_position is not None:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] CM->KUKSA | SoC: {current_soc:.2f}%, Dist: {current_position:.2f} m", end="")

                updates = {
                    "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current": Datapoint(current_soc),
                    "Vehicle.TraveledDistance": Datapoint(current_position),
                }
                kuksa.set_current_values(updates)
                print(" | Published.", end="")
            else:
                print("Waiting for valid data from CarMaker...", end="")

            # === PART 2: KUKSA to CarMaker (NEW LOGIC) ===
            try:
                # The agent only publishes statuses for reachable stations.
                station_statuses = kuksa.get_current_values(STATION_VSS_SIGNALS)
                
                if station_statuses:
                    print(" | KUKSA->CM | ", end="")
                    station_booked = False
                    
                    # Create a simple dictionary of reachable VSS paths and their statuses for easy lookup.
                    reachable_status_map = {
                        vss_path: int(dp.value)
                        for vss_path, dp in station_statuses.items() if dp and dp.value is not None
                    }
                    print(f"Reachable stations found: {list(reachable_status_map.keys())} ", end="")

                    # ### --- NEW BOOKING STRATEGY --- ###
                    # Iterate backwards from the FARTHEST station (CH5) to the CLOSEST (CH1).
                    # Book the first one we find that is BOTH reachable AND available.
                    for vss_path in reversed(STATION_VSS_SIGNALS):
                        # Check 1: Is this station in the list of reachable stations from the agent?
                        # Check 2: If yes, is its status '1' (available)?
                        if vss_path in reachable_status_map and reachable_status_map[vss_path] == 1:
                            
                            station_to_book = VSS_TO_CM_MAP[vss_path]
                            charge_value_to_write = STATION_TO_CHARGE_VALUE_MAP.get(station_to_book)

                            if charge_value_to_write is not None:
                                if charge_value_to_write == 1:
                                    cm.DVA_write(charge_Request, 1)
                                    cm.DVA_write(c2, 0); cm.DVA_write(c3, 0); cm.DVA_write(c4, 0); cm.DVA_write(c5, 0)
                                elif charge_value_to_write == 5:
                                    cm.DVA_write(charge_Request, 5)
                                    if current_soc >=30:
                                        cm.DVA_write(c1, 0); cm.DVA_write(c3, 0); cm.DVA_write(c4, 0); cm.DVA_write(c5, 0)
                                elif charge_value_to_write == 8:
                                    cm.DVA_write(charge_Request, 8)
                                    if current_soc >=30:
                                        cm.DVA_write(c1, 0); cm.DVA_write(c2, 0); cm.DVA_write(c4, 0); cm.DVA_write(c5, 0)
                                elif charge_value_to_write == 11:
                                    cm.DVA_write(charge_Request, 11)
                                    if current_soc >=30:
                                        cm.DVA_write(c1, 0); cm.DVA_write(c2, 0); cm.DVA_write(c3, 0); cm.DVA_write(c5, 0)
                                elif charge_value_to_write == 14:
                                    cm.DVA_write(charge_Request, 14)
                                    if current_soc >=30:
                                        cm.DVA_write(c1, 0); cm.DVA_write(c2, 0); cm.DVA_write(c3, 0); cm.DVA_write(c4, 0)
                                if current_soc <=30:
                                    if current_position <=2380:
                                        cm.DVA_write(charge_Request,1)
                                    elif current_position <=12600:
                                        cm.DVA_write(charge_Request,5)
                                    elif current_position <=24800:
                                        cm.DVA_write(charge_Request,8)
                                    elif current_position <=55370:
                                        cm.DVA_write(charge_Request,11)
                                    elif current_position <=95110:
                                        cm.DVA_write(charge_Request,14)
                                
                                print(f"| Booking furthest reachable & available station ({station_to_book.name}). Writing {int(charge_value_to_write)}. ", end="")
                                cm.DVA_release()
                                station_booked = True
                                break # IMPORTANT: Exit the loop after booking the best option.
                            else:
                                print(f"| Error: No charge value map for {station_to_book.name}. ", end="")

                    if not station_booked:
                        print("| No reachable stations are currently available. ", end="")

                else:
                    print(" | KUKSA->CM | No reachable station data from agent yet. ", end="")

            except Exception as e:
                if '404' in str(e) and 'not_found' in str(e):
                    print(" | KUKSA->CM: No station status published by agent yet.", end="")
                else:
                    print(f" Error polling from KUKSA: {e}", end="")
            
            print() 
            time.sleep(3)

except ConnectionRefusedError:
    print("\n Error: Connection to KUKSA databroker refused.")
except KeyboardInterrupt:
    print("\n--- Script stopped by user ---")
finally:
    print("Disconnecting from CarMaker.")
    cm.DVA_release()
    cm.disconnect()