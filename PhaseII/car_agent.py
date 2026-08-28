# kuksa_uagent_client.py (ss12.py)
# Description: A uAgent that subscribes to KUKSA signals, calculates range,
# and communicates with a station agent to get charger availability.
# MODIFIED: It now publishes a combined reachability/availability status for ALL stations.

import time
import threading
import queue
from uagents import Agent, Context, Model
from kuksa_client.grpc import VSSClient, Datapoint

# --- Agent & Communication Configuration ---
RECIPIENT_ADDRESS = "agent1q0v5dg5w50ztl932tgtcqm5ej55gp45hqm82e5suanljawv32er7k37p4dk"

# --- KUKSA Configuration ---
KUKSA_HOST = '127.0.0.1'
KUKSA_PORT = 55555

# --- Vehicle & Route Configuration ---
BATTERY_CAPACITY_KWH = 40.0
TOTAL_TRIP_DISTANCE_KM = 153.0
SOC_THRESHOLD_PERCENT = 15.0  # Minimum SoC to have upon arrival
SOC_BUFFER_PERCENT = 5.0      # Extra safety buffer

# --- Shared State for KUKSA data ---
# This data is updated by the KUKSA thread and read by the uAgent thread.
# A lock is used to ensure thread safety.
shared_vehicle_data = {
    "soc": 0.0,
    "distance_m": 0.0,
    "consumption_kwh_km": 0.18 # Default/initial consumption value
}
data_lock = threading.Lock()

# --- Message Models ---
class StatusRequest(Model):
    """A request message to ask for station statuses."""
    pass

class StationStatusResponse(Model):
    """A response message containing the statuses of all stations."""
    statuses: dict[str, str]

# Thread-safe queue to communicate from KUKSA thread to uAgent thread
request_queue = queue.Queue()

def run_kuksa_listener(q: queue.Queue):
    """The KUKSA logic that runs in a separate thread."""
    previous_soc = None
    previous_distance_m = None
    last_known_consumption_kwh_km = shared_vehicle_data["consumption_kwh_km"] # Start with default

    print("--- KUKSA Listener Thread Started ---")
    try:
        with VSSClient(host=KUKSA_HOST, port=KUKSA_PORT) as client:
            print(" KUKSA: Successfully connected. Subscribing...")
            for updates in client.subscribe_current_values([
                "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
                "Vehicle.TraveledDistance"
            ]):
                current_soc = updates.get("Vehicle.Powertrain.TractionBattery.StateOfCharge.Current", Datapoint(value=previous_soc)).value
                current_distance_m = updates.get("Vehicle.TraveledDistance", Datapoint(value=previous_distance_m)).value

                if current_soc is None or current_distance_m is None:
                    continue
                if previous_soc is None:
                    previous_soc, previous_distance_m = current_soc, current_distance_m
                    continue

                # Dynamically calculate consumption
                if current_distance_m > previous_distance_m and previous_soc > current_soc:
                    delta_distance_m = current_distance_m - previous_distance_m
                    delta_soc_percent = previous_soc - current_soc
                    if delta_distance_m > 0 and delta_soc_percent > 0:
                        energy_consumed_kwh = (delta_soc_percent / 100.0) * BATTERY_CAPACITY_KWH
                        delta_distance_km = delta_distance_m / 1000.0
                        last_known_consumption_kwh_km = energy_consumed_kwh / delta_distance_km

                # Calculate current range
                min_soc_reserve = SOC_THRESHOLD_PERCENT + SOC_BUFFER_PERCENT
                usable_soc_percent = max(0.0, current_soc - min_soc_reserve)
                usable_energy_kwh = (usable_soc_percent / 100.0) * BATTERY_CAPACITY_KWH
                
                reachable_distance_km = 0.0
                if last_known_consumption_kwh_km > 0:
                    reachable_distance_km = usable_energy_kwh / last_known_consumption_kwh_km
                
                remaining_trip_dist_km = max(0.0, (TOTAL_TRIP_DISTANCE_KM * 1000.0 - current_distance_m) / 1000.0)
                print(f" KUKSA: SoC: {current_soc:.1f}%, Range: {reachable_distance_km:.1f} km, Trip Left: {remaining_trip_dist_km:.1f} km")

                # Update the shared data for the agent thread to use
                with data_lock:
                    shared_vehicle_data["soc"] = current_soc
                    shared_vehicle_data["distance_m"] = current_distance_m
                    shared_vehicle_data["consumption_kwh_km"] = last_known_consumption_kwh_km

                # Check if a charge is needed
                if remaining_trip_dist_km > reachable_distance_km:
                    print(" KUKSA: WARNING: Range insufficient. Requesting station status.")
                    q.put("REQUEST_STATUS")
                
                previous_soc, previous_distance_m = current_soc, current_distance_m
                time.sleep(2)
    except Exception as e:
        print(f"KUKSA THREAD ERROR: {e}")

# --- uAgent Definition ---
vehicle_agent = Agent(
    name="vehicle_agent",
    port=8000,
    seed="vehicle agent secret phrase",
    endpoint=["http://127.0.0.1:8000/submit"],
)

@vehicle_agent.on_event("startup")
async def startup(ctx: Context):
    ctx.logger.info(f"Vehicle Agent started.")
    ctx.logger.info(f"My address is: {ctx.agent.address}")

@vehicle_agent.on_interval(period=3.0)
async def check_kuksa_requests(ctx: Context):
    try:
        request = request_queue.get_nowait()
        if request == "REQUEST_STATUS":
            ctx.logger.info("Charge needed! Sending status request to station agent...")
            await ctx.send(RECIPIENT_ADDRESS, StatusRequest())
    except queue.Empty:
        pass

@vehicle_agent.on_message(model=StationStatusResponse)
async def handle_status_response(ctx: Context, sender: str, msg: StationStatusResponse):
    """
    Handles the station status response.
    It calculates reachability and then publishes a combined status for ALL stations.
    - If reachable: publishes actual status (1=available, 2=occupied, etc.)
    - If unreachable: publishes 0
    """
    ctx.logger.info(f"Received station status from {sender}: {msg.statuses}")

    # Define charging station locations (distance from trip start in meters)
    stations = [
        {'id': 'ch1', 'distance_m': 2380},   # 2.38 km
        {'id': 'ch2', 'distance_m': 12620},   # 12.6 km
        {'id': 'ch3', 'distance_m': 24300},   # 24.3 km
        {'id': 'ch4', 'distance_m': 55370},  # 55.37 km
        {'id': 'ch5', 'distance_m': 95110}   # 95.11 km
    ]

    # --- REACHABILITY LOGIC ---
    with data_lock:
        current_soc = shared_vehicle_data["soc"]
        current_distance_m = shared_vehicle_data["distance_m"]
        avg_consumption_kwh_km = shared_vehicle_data["consumption_kwh_km"]

    reachable_station_ids = set()
    ctx.logger.info("--- Calculating reachable stations ---")
    ctx.logger.info(f"Current State: SoC={current_soc:.1f}%, Position={current_distance_m/1000.0:.1f}km")

    for s in stations:
        if s['distance_m'] > current_distance_m:
            distance_to_station_km = (s['distance_m'] - current_distance_m) / 1000.0
            energy_needed_kwh = distance_to_station_km * avg_consumption_kwh_km
            soc_needed = (energy_needed_kwh / BATTERY_CAPACITY_KWH) * 100.0
            total_soc_required = soc_needed
            
            if current_soc >= total_soc_required and total_soc_required >0:
                reachable_station_ids.add(s['id'])
                ctx.logger.info(f"  -> Station {s['id']} is REACHABLE (Requires {total_soc_required:.1f}% SoC)")    
            else:
                ctx.logger.info(f"  -> Station {s['id']} is NOT REACHABLE (Requires {total_soc_required:.1f}% SoC)")
    
    ctx.logger.info("--- Calculation complete ---")

    # --- KUKSA PUBLISHING (MODIFIED LOGIC) ---
    status_to_int_map = { 'available': 1, 'occupied': 2, 'booked': 3, 'out_of_service': 4 }
    station_id_to_vss_map = {
        'ch1': 'Vehicle.Chassis.Axle.Row1.Wheel.Left.Tire.Pressure',
        'ch2': 'Vehicle.Chassis.Axle.Row1.Wheel.Right.Tire.Pressure',
        'ch3': 'Vehicle.Chassis.Axle.Row2.Wheel.Left.Tire.Pressure',
        'ch4': 'Vehicle.Chassis.Axle.Row2.Wheel.Right.Tire.Pressure',
        'ch5': 'Vehicle.Chassis.Brake.PedalPosition',
    }

    kuksa_updates = {}
    # Iterate through ALL stations that we have a status for.
    for station_id, status_str in msg.statuses.items():
        vss_path = station_id_to_vss_map.get(station_id)
        if vss_path:
            # Check if this station was calculated to be reachable
            if station_id in reachable_station_ids:
                # If reachable, use its actual status (1 for available, 2 for occupied etc.)
                status_int = status_to_int_map.get(status_str.lower(), 0) # Default to 0 if status unknown
                ctx.logger.info(f"Publishing status for REACHABLE station {station_id}: {status_str} -> {status_int}")
            else:
                # If NOT reachable, publish 0 to signify it's not a valid option for booking.
                status_int = 0 # 0 means UNREACHABLE
                ctx.logger.info(f"Publishing status for UNREACHABLE station {station_id}: -> {status_int}")
            
            kuksa_updates[vss_path] = Datapoint(status_int)

    if kuksa_updates:
        try:
            with VSSClient(host=KUKSA_HOST, port=KUKSA_PORT) as client:
                client.set_current_values(kuksa_updates)
                ctx.logger.info(f"Successfully published combined status for all stations to KUKSA.")
        except Exception as e:
            ctx.logger.error(f"Failed to publish to KUKSA: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    kuksa_thread = threading.Thread(target=run_kuksa_listener, args=(request_queue,), daemon=True)
    kuksa_thread.start()
    vehicle_agent.run()