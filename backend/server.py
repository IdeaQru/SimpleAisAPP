# ============================================================================
# POSEIDON EYE - AIS VESSEL MONITORING SYSTEM
# Server Backend with Real-time MongoDB Persistence & Enhanced Logging
# ============================================================================

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import asyncio
import socket
import json
import logging
import os
from pyais import decode
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# LOGGING CONFIGURATION - Enhanced with colors and formatting
# ============================================================================
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for detailed logging
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# MONGODB CONNECTION with Connection Pooling
# ============================================================================
MONGODB_URL = os.environ.get('MONGODB_URL', 'mongodb://127.0.0.1:27017')
logger.info(f"MongoDB URL configured: {MONGODB_URL}")

client = AsyncIOMotorClient(
    MONGODB_URL,
    serverSelectionTimeoutMS=5000,  # 5 second timeout
    connectTimeoutMS=10000,
    maxPoolSize=50,
    minPoolSize=10
)
db = client.ais_monitoring

# ============================================================================
# GLOBAL STATE
# ============================================================================
connection_config = {"host": "165.154.228.42", "port": 5000}
connection_active = False
active_websockets = set()
last_alert_sent = None
alert_cooldown = 300
ais_task = None
vessel_update_counter = 0  # Track total updates
vessel_insert_counter = 0  # Track new vessels

# ============================================================================
# PYDANTIC MODELS
# ============================================================================
class ConnectionConfig(BaseModel):
    host: str
    port: int

class ConnectionStatus(BaseModel):
    active: bool
    host: str
    port: int
    vessel_count: int
    updates_count: int
    inserts_count: int

class Vessel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    mmsi: int
    lat: Optional[float] = None
    lon: Optional[float] = None
    speed: Optional[float] = None
    course: Optional[float] = None
    heading: Optional[int] = None
    name: Optional[str] = None
    callsign: Optional[str] = None
    ship_type: Optional[int] = None
    ship_type_text: Optional[str] = None
    destination: Optional[str] = None
    eta: Optional[str] = None
    draught: Optional[float] = None
    dim_bow: Optional[int] = None
    dim_stern: Optional[int] = None
    dim_port: Optional[int] = None
    dim_starboard: Optional[int] = None
    imo: Optional[int] = None
    status: Optional[int] = None
    status_text: Optional[str] = None
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def get_ship_type_text(ship_type: int) -> str:
    """Convert ship type code to readable text"""
    ship_types = {
        0: "Not available", 20: "Wing in ground", 30: "Fishing",
        31: "Towing", 32: "Towing large", 33: "Dredging",
        34: "Diving ops", 35: "Military ops", 36: "Sailing",
        37: "Pleasure Craft", 40: "High speed craft", 50: "Pilot Vessel",
        51: "Search and Rescue", 52: "Tug", 53: "Port Tender",
        54: "Anti-pollution", 55: "Law Enforcement", 58: "Medical Transport",
        59: "Noncombatant", 60: "Passenger", 70: "Cargo",
        80: "Tanker", 90: "Other"
    }
    return ship_types.get(ship_type, f"Unknown ({ship_type})")

def get_status_text(status: int) -> str:
    """Convert navigation status to text"""
    statuses = {
        0: "Under way using engine", 1: "At anchor",
        2: "Not under command", 3: "Restricted manoeuvrability",
        4: "Constrained by draught", 5: "Moored",
        6: "Aground", 7: "Engaged in Fishing",
        8: "Under way sailing", 15: "Not defined"
    }
    return statuses.get(status, "Unknown")

# ============================================================================
# EMAIL ALERT FUNCTION
# ============================================================================
async def send_alert_email(subject: str, body: str):
    """Send alert email via Gmail SMTP"""
    global last_alert_sent

    if last_alert_sent:
        time_since_last = (datetime.now(timezone.utc) - last_alert_sent).total_seconds()
        if time_since_last < alert_cooldown:
            logger.debug(f"⏳ Alert cooldown active. Time remaining: {alert_cooldown - time_since_last:.0f}s")
            return

    try:
        smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.environ.get('SMTP_PORT', 587))
        smtp_user = os.environ.get('SMTP_USER')
        smtp_password = os.environ.get('SMTP_PASSWORD')
        alert_email = os.environ.get('ALERT_EMAIL')

        if not all([smtp_user, smtp_password, alert_email]):
            logger.warning("⚠️ Email configuration incomplete. Skipping alert.")
            return

        message = MIMEMultipart('alternative')
        message['Subject'] = subject
        message['From'] = smtp_user
        message['To'] = alert_email

        html_body = f"""
        <html>
          <body style="font-family: Arial, sans-serif; padding: 20px; background-color: #f5f5f5;">
            <div style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 10px; padding: 30px;">
              <h1 style="color: #EF4444;">⚠️ AIS Connection Alert</h1>
              <p style="font-size: 16px;">{body}</p>
              <hr style="margin: 20px 0; border: none; border-top: 1px solid #E5E7EB;">
              <p><strong>Host:</strong> {connection_config['host']}</p>
              <p><strong>Port:</strong> {connection_config['port']}</p>
              <p><strong>Time:</strong> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
              <p style="margin-top: 20px; color: #6B7280; font-size: 12px;">Poseidon Eye - AIS Monitoring System</p>
            </div>
          </body>
        </html>
        """

        html_part = MIMEText(html_body, 'html')
        message.attach(html_part)

        await aiosmtplib.send(
            message,
            hostname=smtp_host,
            port=smtp_port,
            username=smtp_user,
            password=smtp_password,
            start_tls=True,
        )

        last_alert_sent = datetime.now(timezone.utc)
        logger.info(f"📧 Alert email sent successfully to {alert_email}")

    except Exception as e:
        logger.error(f"❌ Failed to send alert email: {e}")

# ============================================================================
# WEBSOCKET BROADCAST FUNCTION
# ============================================================================
async def broadcast_to_websockets(data: dict):
    """Broadcast data to all connected WebSocket clients"""
    if not active_websockets:
        return

    message = json.dumps(data)
    disconnected = set()

    for websocket in active_websockets:
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.debug(f"WebSocket send error: {e}")
            disconnected.add(websocket)

    # Clean up disconnected websockets
    for ws in disconnected:
        active_websockets.discard(ws)

    if disconnected:
        logger.debug(f"Removed {len(disconnected)} disconnected websocket(s)")

# ============================================================================
# AIS DATA PROCESSING - MAIN FUNCTION
# ============================================================================
async def process_ais_data():
    """Process AIS data from TCP connection with real-time MongoDB persistence"""
    global connection_active, vessel_update_counter, vessel_insert_counter

    host = connection_config["host"]
    port = connection_config["port"]

    logger.info(f"🚀 Starting AIS connection to {host}:{port}")

    while True:
        sock = None
        try:
            logger.info(f"🔌 Connecting to AIS source at {host}:{port}")

            # Create TCP socket connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            sock.settimeout(None)  # Remove timeout after connection

            connection_active = True
            logger.info(f"✅ Connected to AIS source at {host}:{port}")

            buffer = ""
            message_count = 0

            while connection_active:
                try:
                    data = sock.recv(4096)
                    if not data:
                        logger.warning("⚠️ Connection closed by remote host")
                        break

                    # Decode bytes to string
                    buffer += data.decode('utf-8', errors='ignore')

                    # Process complete NMEA sentences
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()

                        if not line or not line.startswith('!'):
                            continue

                        message_count += 1

                        try:
                            # Decode NMEA AIS message
                            decoded = decode(line)
                            msg_dict = decoded.asdict()

                            mmsi = msg_dict.get('mmsi')
                            if not mmsi:
                                continue

                            msg_type = msg_dict.get('msg_type')
                            vessel_data = {"mmsi": mmsi}

                            # Position reports (Type 1, 2, 3)
                            if msg_type in [1, 2, 3]:
                                vessel_data.update({
                                    "lat": msg_dict.get('lat'),
                                    "lon": msg_dict.get('lon'),
                                    "speed": msg_dict.get('speed'),
                                    "course": msg_dict.get('course'),
                                    "heading": msg_dict.get('heading'),
                                    "status": msg_dict.get('status'),
                                    "status_text": get_status_text(msg_dict.get('status', 15)),
                                    "last_updated": datetime.now(timezone.utc)
                                })

                            # Static and voyage data (Type 5)
                            elif msg_type == 5:
                                vessel_data.update({
                                    "name": msg_dict.get('shipname', '').strip(),
                                    "callsign": msg_dict.get('callsign', '').strip(),
                                    "ship_type": msg_dict.get('ship_type'),
                                    "ship_type_text": get_ship_type_text(msg_dict.get('ship_type', 0)),
                                    "destination": msg_dict.get('destination', '').strip(),
                                    "eta": str(msg_dict.get('eta')) if msg_dict.get('eta') else None,
                                    "draught": msg_dict.get('draught'),
                                    "dim_bow": msg_dict.get('to_bow'),
                                    "dim_stern": msg_dict.get('to_stern'),
                                    "dim_port": msg_dict.get('to_port'),
                                    "dim_starboard": msg_dict.get('to_starboard'),
                                    "imo": msg_dict.get('imo'),
                                    "last_updated": datetime.now(timezone.utc)
                                })

                            # Static data (Type 24)
                            elif msg_type == 24:
                                vessel_data.update({
                                    "name": msg_dict.get('shipname', '').strip(),
                                    "callsign": msg_dict.get('callsign', '').strip(),
                                    "ship_type": msg_dict.get('ship_type'),
                                    "ship_type_text": get_ship_type_text(msg_dict.get('ship_type', 0)),
                                    "last_updated": datetime.now(timezone.utc)
                                })

                            # ==========================================
                            # SAVE TO MONGODB - Real-time Persistence
                            # ==========================================
                            if vessel_data and mmsi and len(vessel_data) > 1:
                                # Remove None values
                                vessel_data = {k: v for k, v in vessel_data.items() if v is not None}

                                # Convert datetime to ISO string for MongoDB
                                if 'last_updated' in vessel_data:
                                    vessel_data['last_updated'] = vessel_data['last_updated'].isoformat()

                                try:
                                    # Upsert to MongoDB with error handling
                                    result = await db.vessels.update_one(
                                        {"mmsi": mmsi},
                                        {"$set": vessel_data},
                                        upsert=True
                                    )

                                    # Log and track statistics
                                    if result.upserted_id:
                                        vessel_insert_counter += 1
                                        vessel_name = vessel_data.get('name', f'MMSI-{mmsi}')
                                        logger.info(f"✅ NEW VESSEL INSERTED: {vessel_name} (MMSI: {mmsi}) | Type: {msg_type} | Total: {vessel_insert_counter}")
                                    elif result.modified_count > 0:
                                        vessel_update_counter += 1
                                        if vessel_update_counter % 100 == 0:  # Log every 100 updates
                                            logger.info(f"📊 Progress: {vessel_update_counter} updates | {vessel_insert_counter} new vessels")

                                    # Broadcast to WebSocket clients
                                    await broadcast_to_websockets({
                                        "type": "vessel_update",
                                        "data": vessel_data
                                    })

                                except Exception as db_error:
                                    logger.error(f"❌ MongoDB write failed for MMSI {mmsi}: {db_error}")
                                    continue

                        except Exception as decode_error:
                            # Silent on decode errors (too noisy)
                            if message_count % 1000 == 0:
                                logger.debug(f"Processed {message_count} messages ({vessel_insert_counter} vessels)")
                            continue

                except Exception as read_error:
                    logger.error(f"❌ Error reading data: {read_error}")
                    break

            # Connection closed
            if sock:
                sock.close()
            connection_active = False

            logger.warning(f"⚠️ AIS connection lost to {host}:{port}")
            await send_alert_email(
                subject="🚨 AIS Connection Lost - Action Required",
                body=f"The AIS data stream from {host}:{port} has been disconnected. System will attempt to reconnect automatically."
            )

        except Exception as e:
            logger.error(f"❌ AIS connection error: {e}")
            connection_active = False

            if sock:
                try:
                    sock.close()
                except:
                    pass

            await send_alert_email(
                subject="🚨 AIS Connection Error - Action Required",
                body=f"Failed to connect to AIS source at {host}:{port}. Error: {str(e)}. Retrying in 5 seconds."
            )

            await asyncio.sleep(5)

# ============================================================================
# LIFESPAN CONTEXT MANAGER - Startup & Shutdown
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown events"""
    global ais_task

    logger.info("=" * 70)
    logger.info("🌊 POSEIDON EYE - AIS VESSEL MONITORING SYSTEM")
    logger.info("=" * 70)

    # ========================================
    # STARTUP: Test MongoDB Connection
    # ========================================
    try:
        logger.info("🔍 Testing MongoDB connection...")
        await client.admin.command('ping')
        logger.info("✅ MongoDB connected successfully!")

        # Create index on mmsi for better query performance
        await db.vessels.create_index("mmsi", unique=True)
        logger.info("✅ MongoDB index created on 'mmsi' field")

        # Get initial vessel count
        initial_count = await db.vessels.count_documents({})
        logger.info(f"📊 Current vessels in database: {initial_count}")

        # Get database stats
        stats = await db.command("dbStats")
        db_size_mb = stats.get('dataSize', 0) / (1024 * 1024)
        logger.info(f"💾 Database size: {db_size_mb:.2f} MB")

    except Exception as e:
        logger.error("=" * 70)
        logger.error(f"❌ CRITICAL: MongoDB connection FAILED!")
        logger.error(f"❌ Error: {e}")
        logger.error(f"❌ Connection string: {MONGODB_URL}")
        logger.error("❌ Data will NOT be saved to database!")
        logger.error("=" * 70)

    # ========================================
    # STARTUP: Start AIS Data Processing
    # ========================================
    ais_task = asyncio.create_task(process_ais_data())
    logger.info("🚀 AIS data processing task started")
    logger.info("=" * 70)

    yield  # Application runs here

    # ========================================
    # SHUTDOWN: Clean up resources
    # ========================================
    logger.info("=" * 70)
    logger.info("🛑 Shutting down Poseidon Eye...")

    global connection_active
    connection_active = False

    if ais_task:
        ais_task.cancel()
        try:
            await ais_task
        except asyncio.CancelledError:
            logger.info("✅ AIS task cancelled successfully")

    # Close MongoDB connection
    client.close()
    logger.info("✅ MongoDB connection closed")

    # Final statistics
    logger.info(f"📊 Final Stats: {vessel_insert_counter} vessels inserted, {vessel_update_counter} updates processed")
    logger.info("=" * 70)

# ============================================================================
# CREATE FASTAPI APP
# ============================================================================
app = FastAPI(
    title="Poseidon Eye - AIS Monitoring API",
    description="Real-time AIS vessel tracking and monitoring system",
    version="1.0.0",
    lifespan=lifespan
)

# Create router with /api prefix
api_router = APIRouter(prefix="/api")

# ============================================================================
# API ENDPOINTS
# ============================================================================

@api_router.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Poseidon Eye - AIS Vessel Monitoring API",
        "version": "1.0.0",
        "status": "online"
    }

@api_router.get("/vessels", response_model=List[Vessel])
async def get_vessels():
    """Get all vessels from database"""
    try:
        vessels = await db.vessels.find(
            {}, 
            {
                "_id": 0, "mmsi": 1, "lat": 1, "lon": 1,
                "speed": 1, "course": 1, "heading": 1,
                "name": 1, "callsign": 1, "ship_type": 1,
                "ship_type_text": 1, "status": 1,
                "status_text": 1, "last_updated": 1,
                "destination": 1, "eta": 1, "draught": 1,
                "dim_bow": 1, "dim_stern": 1, "dim_port": 1,
                "dim_starboard": 1, "imo": 1
            }
        ).to_list(1000)

        # Convert ISO string timestamps back to datetime
        for vessel in vessels:
            if isinstance(vessel.get('last_updated'), str):
                vessel['last_updated'] = datetime.fromisoformat(vessel['last_updated'])

        logger.debug(f"📤 Returned {len(vessels)} vessels")
        return vessels

    except Exception as e:
        logger.error(f"❌ Error fetching vessels: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch vessels: {str(e)}")

@api_router.get("/vessels/{mmsi}", response_model=Vessel)
async def get_vessel(mmsi: int):
    """Get vessel by MMSI"""
    try:
        vessel = await db.vessels.find_one({"mmsi": mmsi}, {"_id": 0})

        if not vessel:
            raise HTTPException(status_code=404, detail=f"Vessel with MMSI {mmsi} not found")

        # Convert ISO string timestamp
        if isinstance(vessel.get('last_updated'), str):
            vessel['last_updated'] = datetime.fromisoformat(vessel['last_updated'])

        logger.debug(f"📤 Returned vessel {mmsi}")
        return vessel

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error fetching vessel {mmsi}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch vessel: {str(e)}")

@api_router.post("/connection/config")
async def set_connection_config(config: ConnectionConfig):
    """Set AIS connection configuration"""
    global connection_config

    connection_config["host"] = config.host
    connection_config["port"] = config.port

    logger.info(f"⚙️ Connection config updated: {config.host}:{config.port}")

    return {
        "status": "success",
        "message": "Configuration updated. Restart required for changes to take effect.",
        "config": connection_config
    }

@api_router.get("/connection/status", response_model=ConnectionStatus)
async def get_connection_status():
    """Get current connection status and statistics"""
    try:
        vessel_count = await db.vessels.count_documents({})

        return ConnectionStatus(
            active=connection_active,
            host=connection_config["host"],
            port=connection_config["port"],
            vessel_count=vessel_count,
            updates_count=vessel_update_counter,
            inserts_count=vessel_insert_counter
        )
    except Exception as e:
        logger.error(f"❌ Error getting connection status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api_router.post("/connection/test-alert")
async def test_alert_email():
    """Test email alert functionality"""
    try:
        await send_alert_email(
            subject="✅ Test Alert - AIS Monitoring System",
            body="This is a test email from Poseidon Eye. If you receive this, email alerts are configured correctly!"
        )
        return {
            "status": "success",
            "message": f"Test email sent to {os.environ.get('ALERT_EMAIL', 'configured email')}"
        }
    except Exception as e:
        logger.error(f"❌ Test email failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send test email: {str(e)}")

# ============================================================================
# DEBUG ENDPOINTS
# ============================================================================

@api_router.get("/debug/mongodb")
async def debug_mongodb():
    """Debug MongoDB connection and database info"""
    try:
        # Test ping
        await client.admin.command('ping')

        # Get database list
        db_list = await client.list_database_names()

        # Get collections in current database
        collections = await db.list_collection_names()

        # Count vessels
        vessel_count = await db.vessels.count_documents({})

        # Get sample vessels (first 5)
        sample_vessels = await db.vessels.find({}).limit(5).to_list(5)

        # Get database stats
        stats = await db.command("dbStats")

        return {
            "status": "connected",
            "mongodb_url": MONGODB_URL.split('@')[-1] if '@' in MONGODB_URL else MONGODB_URL,
            "databases": db_list,
            "current_database": db.name,
            "collections": collections,
            "vessel_count": vessel_count,
            "sample_mmsi": [v.get('mmsi') for v in sample_vessels],
            "database_size_mb": round(stats.get('dataSize', 0) / (1024 * 1024), 2),
            "storage_size_mb": round(stats.get('storageSize', 0) / (1024 * 1024), 2),
            "indexes": stats.get('indexes', 0)
        }
    except Exception as e:
        logger.error(f"❌ MongoDB debug error: {e}")
        return {
            "status": "error",
            "error": str(e),
            "mongodb_url": MONGODB_URL
        }

@api_router.post("/debug/test-write")
async def test_mongodb_write():
    """Test MongoDB write operation"""
    try:
        test_mmsi = 999999999
        test_data = {
            "mmsi": test_mmsi,
            "name": "TEST_VESSEL_POSEIDON",
            "lat": -6.2088,  # Surabaya coordinates
            "lon": 106.8456,
            "speed": 10.5,
            "course": 180.0,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }

        # Try to write
        result = await db.vessels.update_one(
            {"mmsi": test_mmsi},
            {"$set": test_data},
            upsert=True
        )

        # Verify write
        found = await db.vessels.find_one({"mmsi": test_mmsi})

        # Get total count
        total_count = await db.vessels.count_documents({})

        logger.info(f"✅ Test write successful for MMSI {test_mmsi}")

        return {
            "status": "success",
            "write_result": {
                "matched_count": result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id": str(result.upserted_id) if result.upserted_id else None
            },
            "test_vessel_found": found is not None,
            "test_vessel_data": found,
            "total_vessels_in_db": total_count,
            "message": "✅ MongoDB write operation successful!"
        }
    except Exception as e:
        logger.error(f"❌ Test write failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "message": "❌ MongoDB write operation failed!"
        }

# ============================================================================
# WEBSOCKET ENDPOINT
# ============================================================================

@api_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time vessel updates"""
    await websocket.accept()
    active_websockets.add(websocket)
    client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
    logger.info(f"🔌 WebSocket connected from {client_info} | Total: {len(active_websockets)}")

    try:
        # Send initial vessel data
        vessels = await db.vessels.find(
            {}, 
            {
                "_id": 0, "mmsi": 1, "lat": 1, "lon": 1,
                "speed": 1, "course": 1, "heading": 1,
                "name": 1, "callsign": 1, "ship_type": 1,
                "ship_type_text": 1, "status": 1,
                "status_text": 1, "last_updated": 1
            }
        ).to_list(1000)

        # Convert datetime to ISO string for JSON serialization
        for vessel in vessels:
            if isinstance(vessel.get('last_updated'), datetime):
                vessel['last_updated'] = vessel['last_updated'].isoformat()

        initial_message = json.dumps({
            "type": "initial_data",
            "data": vessels,
            "count": len(vessels)
        })

        await websocket.send_text(initial_message)
        logger.debug(f"📤 Sent {len(vessels)} initial vessels to WebSocket client")

        # Keep connection alive and receive messages
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info(f"🔌 WebSocket disconnected from {client_info}")
    except Exception as e:
        logger.error(f"❌ WebSocket error from {client_info}: {e}")
    finally:
        active_websockets.discard(websocket)
        logger.info(f"🔌 WebSocket removed | Total: {len(active_websockets)}")

# ============================================================================
# INCLUDE ROUTER & MIDDLEWARE
# ============================================================================

# Include API router
app.include_router(api_router)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
