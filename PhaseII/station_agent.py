# station_status_agent.py
# Description: A uAgent that provides station status and handles booking requests.

from uagents import Agent, Context, Model

# --- Message Models ---
# These models MUST be identical in the client script.

class StatusRequest(Model):
    """A request message to ask for station statuses."""
    pass

class StationStatusResponse(Model):
    """A response message containing the statuses of all stations."""
    statuses: dict[str, str]

# NEW: Add models for the booking process
class BookStationRequest(Model):
    """A request to book a specific charging station."""
    station_id: str

class BookingConfirmation(Model):
    """A confirmation response for a booking request."""
    success: bool
    message: str

# --- Agent Definition ---
station_agent = Agent(
    name="station_agent",
    port=8001,
    seed="station agent secret phrase",
    endpoint=["http://127.0.0.1:8001/submit"],
)

# This now represents the LIVE status of the chargers
STATION_AVAILABILITY = {
    'ch1': 'available',
    'ch2': 'available',
    'ch3': 'available',
    'ch4': 'available',
    'ch5': 'available'
}

@station_agent.on_event("startup")
async def startup(ctx: Context):
    ctx.logger.info(f"Station Status Agent started.")
    ctx.logger.info(f"My address is: {ctx.agent.address}")
    ctx.logger.info("Waiting for status and booking requests...")

@station_agent.on_message(model=StatusRequest)
async def handle_status_request(ctx: Context, sender: str, msg: StatusRequest):
    ctx.logger.info(f"Received status request from {sender}.")
    
    await ctx.send(sender, StationStatusResponse(statuses=STATION_AVAILABILITY))
    ctx.logger.info(f"Sent station status response: {STATION_AVAILABILITY}")

# NEW: Add a handler for booking requests
@station_agent.on_message(model=BookStationRequest)
async def handle_booking_request(ctx: Context, sender: str, msg: BookStationRequest):
    ctx.logger.info(f"Received booking request message: {msg}")
    
    station_id = msg.station_id
    
    # Check if the requested station is available
    if station_id in STATION_AVAILABILITY and STATION_AVAILABILITY[station_id] == 'available':
        # Mark the station as booked
        STATION_AVAILABILITY[station_id] = 'booked'
        
        # Create and send a confirmation message
        confirmation = BookingConfirmation(
            success=True, 
            message=f"Booking for station {station_id} is confirmed."
        )
        await ctx.send(sender, confirmation)
        ctx.logger.info(f"Sent booking confirmation: {confirmation.message}")
        ctx.logger.info(f"Updated station statuses: {STATION_AVAILABILITY}")
    else:
        # Station is not available, send a failure message
        confirmation = BookingConfirmation(
            success=False, 
            message=f"Booking failed. Station {station_id} is not available."
        )
        await ctx.send(sender, confirmation)
        ctx.logger.info(f"Sent booking failure: {confirmation.message}")

if __name__ == "__main__":
    station_agent.run()