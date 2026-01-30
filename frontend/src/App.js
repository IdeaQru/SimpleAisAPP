import { useState, useEffect, useCallback } from "react";
import "@/App.css";
import { MapContainer, TileLayer, Marker, Popup, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import axios from "axios";
import { Ship, Anchor, Settings, Signal, WifiOff, ChevronLeft, ChevronRight, X } from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000';
const API = `${BACKEND_URL}/api`;

// Perbaiki WS_URL generation
const WS_URL = BACKEND_URL.replace(/^https:/, 'wss:').replace(/^http:/, 'ws:');

console.log('Backend URL:', BACKEND_URL);
console.log('API URL:', API);
console.log('WebSocket URL:', `${WS_URL}/api/ws`);

// Custom ship icon
const createShipIcon = (color = "#10B981") => {
  return L.divIcon({
    className: 'custom-ship-marker',
    html: `
      <div style="
        width: 24px;
        height: 24px;
        background: ${color};
        border: 2px solid #020617;
        border-radius: 50%;
        box-shadow: 0 0 10px ${color}80;
        display: flex;
        align-items: center;
        justify-content: center;
        animation: pulse 2s infinite;
      ">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#020617" stroke-width="2">
          <path d="M2 21c.6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1 .6.5 1.2 1 2.5 1 2.5 0 2.5-2 5-2 1.3 0 1.9.5 2.5 1"/>
          <path d="M19.38 20A11.6 11.6 0 0 0 21 14l-9-4-9 4c0 2.9.94 5.34 2.81 7.76"/>
          <path d="M19 13V7a2 2 0 0 0-2-2H7a2 2 0 0 0-2 2v6"/>
          <path d="M12 10v4"/>
          <path d="M12 2v3"/>
        </svg>
      </div>
    `,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
};

function MapUpdater({ vessels, selectedVessel }) {
  const map = useMap();
  
  useEffect(() => {
    if (selectedVessel && selectedVessel.lat && selectedVessel.lon) {
      map.flyTo([selectedVessel.lat, selectedVessel.lon], 12, {
        duration: 1.5
      });
    }
  }, [selectedVessel, map]);
  
  return null;
}

function App() {
  const [vessels, setVessels] = useState([]);
  const [selectedVessel, setSelectedVessel] = useState(null);
  const [connectionStatus, setConnectionStatus] = useState(null);
  const [showSettings, setShowSettings] = useState(false);
  const [showVesselDetail, setShowVesselDetail] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [connectionConfig, setConnectionConfig] = useState({
    host: "vps2.osi.my.id",
    port: 6000
  });
  const [ws, setWs] = useState(null);

  // Get vessels with position for map rendering (limit to 200 for performance)
  const vesselsWithPosition = vessels.filter(v => v.lat && v.lon).slice(0, 200);

  // Fetch connection status
  const fetchConnectionStatus = useCallback(async () => {
    try {
      const response = await axios.get(`${API}/connection/status`);
      setConnectionStatus(response.data);
    } catch (e) {
      console.error("Error fetching connection status:", e);
    }
  }, []);

  // Fetch vessels
  const fetchVessels = useCallback(async () => {
    try {
      const response = await axios.get(`${API}/vessels`);
      setVessels(response.data);
    } catch (e) {
      console.error("Error fetching vessels:", e);
    }
  }, []);

  // Setup WebSocket connection
  useEffect(() => {
    let websocket = null;
    let reconnectTimeout = null;
    
    const connectWebSocket = () => {
      try {
        websocket = new WebSocket(`${WS_URL}/api/ws`);
        
        websocket.onopen = () => {
          console.log("WebSocket connected");
          setWs(websocket);
        };
        
        websocket.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            
            if (message.type === "initial_data") {
              setVessels(message.data);
            } else if (message.type === "vessel_update") {
              setVessels(prev => {
                const index = prev.findIndex(v => v.mmsi === message.data.mmsi);
                if (index >= 0) {
                  const updated = [...prev];
                  updated[index] = { ...updated[index], ...message.data };
                  return updated;
                } else {
                  return [...prev, message.data];
                }
              });
              
              // Update selected vessel if it's the one being updated
              setSelectedVessel(prev => {
                if (prev && prev.mmsi === message.data.mmsi) {
                  return { ...prev, ...message.data };
                }
                return prev;
              });
            }
          } catch (err) {
            console.error("Error parsing WebSocket message:", err);
          }
        };
        
        websocket.onerror = (error) => {
          console.error("WebSocket error:", error);
        };
        
        websocket.onclose = () => {
          console.log("WebSocket disconnected. Reconnecting...");
          reconnectTimeout = setTimeout(connectWebSocket, 3000);
        };
      } catch (err) {
        console.error("Error creating WebSocket:", err);
        reconnectTimeout = setTimeout(connectWebSocket, 3000);
      }
    };
    
    // Small delay before connecting to ensure page is loaded
    reconnectTimeout = setTimeout(connectWebSocket, 1000);
    
    return () => {
      if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
      }
      if (websocket) {
        websocket.close();
      }
    };
  }, []);

  // Initial fetch
  useEffect(() => {
    fetchVessels().then(() => setIsLoading(false));
    fetchConnectionStatus();
    
    // Refresh connection status every 10 seconds
    const interval = setInterval(fetchConnectionStatus, 10000);
    return () => clearInterval(interval);
  }, [fetchVessels, fetchConnectionStatus]);

  const handleSaveConnection = async () => {
    try {
      await axios.post(`${API}/connection/config`, connectionConfig);
      toast.success("Connection configuration saved");
      setShowSettings(false);
      setTimeout(() => {
        fetchConnectionStatus();
      }, 2000);
    } catch (e) {
      toast.error("Failed to save configuration");
      console.error(e);
    }
  };

  const handleTestAlert = async () => {
    try {
      const response = await axios.post(`${API}/connection/test-alert`);
      toast.success("Test email sent successfully! Check afif@ppns.ac.id");
    } catch (e) {
      toast.error("Failed to send test email");
      console.error(e);
    }
  };

  const handleVesselClick = (vessel) => {
    setSelectedVessel(vessel);
    setShowVesselDetail(true);
  };

  const getVesselColor = (vessel) => {
    if (!vessel.speed) return "#64748B"; // Gray - offline/no data
    if (vessel.speed < 0.5) return "#F59E0B"; // Amber - stopped
    return "#10B981"; // Green - moving
  };

  const getVesselStatusBadge = (vessel) => {
    if (!vessel.speed) return { variant: "offline", text: "OFFLINE" };
    if (vessel.speed < 0.5) return { variant: "stopped", text: "STOPPED" };
    return { variant: "moving", text: "MOVING" };
  };

  return (
    <div className="app-container">
      {/* Sidebar */}
      <div className={`sidebar ${sidebarCollapsed ? 'collapsed' : ''}`}>
        {/* Header */}
        <div className="sidebar-header">
          <div className="flex items-center gap-3">
            <Ship className="w-8 h-8 text-cyan-400" />
            {!sidebarCollapsed && (
              <div>
                <h1 className="text-xl font-bold uppercase tracking-wider text-slate-100">Poseidon Eye</h1>
                <p className="text-xs text-slate-400 uppercase tracking-widest">AIS Monitor</p>
              </div>
            )}
          </div>
          <button
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            className="collapse-btn"
            data-testid="sidebar-collapse-btn"
          >
            {sidebarCollapsed ? <ChevronRight className="w-5 h-5" /> : <ChevronLeft className="w-5 h-5" />}
          </button>
        </div>

        {/* Connection Status */}
        {!sidebarCollapsed && (
          <div className="connection-status" data-testid="connection-status">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-slate-400 uppercase tracking-widest">Connection</span>
              <button
                onClick={() => setShowSettings(true)}
                className="p-1 hover:bg-slate-800 rounded transition-colors"
                data-testid="settings-btn"
              >
                <Settings className="w-4 h-4 text-slate-400" />
              </button>
            </div>
            <div className="flex items-center gap-2">
              {connectionStatus?.active ? (
                <Signal className="w-4 h-4 text-emerald-400 animate-pulse" />
              ) : (
                <WifiOff className="w-4 h-4 text-red-400" />
              )}
              <span className="text-sm font-mono text-slate-300">
                {connectionStatus?.active ? 'CONNECTED' : 'DISCONNECTED'}
              </span>
            </div>
            <div className="text-xs font-mono text-slate-500 mt-1">
              {connectionStatus?.host}:{connectionStatus?.port}
            </div>
            <div className="text-xs font-mono text-cyan-400 mt-2">
              {connectionStatus?.vessel_count || 0} VESSELS TRACKED
            </div>
          </div>
        )}

        {/* Vessel List */}
        {!sidebarCollapsed && (
          <div className="vessel-list" data-testid="vessel-list">
            <div className="text-xs font-medium text-slate-400 uppercase tracking-widest mb-3">Vessels</div>
            <div className="space-y-2">
              {vesselsWithPosition.map((vessel) => {
                const status = getVesselStatusBadge(vessel);
                return (
                  <div
                    key={vessel.mmsi}
                    onClick={() => handleVesselClick(vessel)}
                    className="vessel-item"
                    data-testid={`vessel-item-${vessel.mmsi}`}
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1">
                          <div
                            className="w-2 h-2 rounded-full"
                            style={{ backgroundColor: getVesselColor(vessel), boxShadow: `0 0 6px ${getVesselColor(vessel)}` }}
                          />
                          <span className="font-bold text-sm text-slate-100 truncate">
                            {vessel.name || `MMSI ${vessel.mmsi}`}
                          </span>
                        </div>
                        <div className="text-xs font-mono text-cyan-400">{vessel.mmsi}</div>
                        {vessel.callsign && (
                          <div className="text-xs text-slate-500">CS: {vessel.callsign}</div>
                        )}
                      </div>
                      <Badge variant={status.variant} className="vessel-badge">
                        {status.text}
                      </Badge>
                    </div>
                    {vessel.speed !== null && (
                      <div className="grid grid-cols-2 gap-2 mt-2 text-xs">
                        <div>
                          <span className="text-slate-500">SPD:</span>
                          <span className="text-cyan-400 ml-1 font-mono">{vessel.speed.toFixed(1)} kn</span>
                        </div>
                        {vessel.course !== null && (
                          <div>
                            <span className="text-slate-500">CRS:</span>
                            <span className="text-cyan-400 ml-1 font-mono">{vessel.course.toFixed(0)}°</span>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
              {vesselsWithPosition.length === 0 && (
                <div className="text-center text-slate-500 text-sm py-8">
                  {isLoading ? "Loading vessels..." : "No vessels detected"}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Map */}
      <div className="map-container">
        {isLoading ? (
          <div className="flex items-center justify-center h-full bg-slate-950">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-cyan-400 mx-auto mb-4"></div>
              <p className="text-slate-400">Loading AIS Monitor...</p>
            </div>
          </div>
        ) : (
          <MapContainer
            center={[0, 100]}
            zoom={5}
            style={{ height: '100%', width: '100%' }}
            zoomControl={true}
          >
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
            />
            {vesselsWithPosition.map((vessel) => (
              <Marker
                key={vessel.mmsi}
                position={[vessel.lat, vessel.lon]}
                icon={createShipIcon(getVesselColor(vessel))}
                eventHandlers={{
                  click: () => handleVesselClick(vessel)
                }}
              >
                <Popup>
                  <div className="text-sm">
                    <div className="font-bold">{vessel.name || `MMSI ${vessel.mmsi}`}</div>
                    <div className="text-xs mt-1">
                      <div>MMSI: {vessel.mmsi}</div>
                      {vessel.speed !== null && <div>Speed: {vessel.speed.toFixed(1)} kn</div>}
                      {vessel.course !== null && <div>Course: {vessel.course.toFixed(0)}°</div>}
                    </div>
                  </div>
                </Popup>
              </Marker>
            ))}
            <MapUpdater vessels={vessels} selectedVessel={selectedVessel} />
          </MapContainer>
        )}
      </div>

      {/* Settings Modal */}
      {showSettings && (
        <div className="modal-overlay" data-testid="settings-modal">
          <Card className="settings-modal">
            <div className="flex items-center justify-between mb-6">
              <h2 className="text-xl font-bold uppercase tracking-wider text-slate-100">Connection Settings</h2>
              <button onClick={() => setShowSettings(false)} className="p-1 hover:bg-slate-800 rounded">
                <X className="w-5 h-5 text-slate-400" />
              </button>
            </div>
            <div className="space-y-4">
              <div>
                <Label htmlFor="host" className="text-xs font-medium text-slate-400 uppercase tracking-widest">Host</Label>
                <Input
                  id="host"
                  value={connectionConfig.host}
                  onChange={(e) => setConnectionConfig({ ...connectionConfig, host: e.target.value })}
                  className="mt-2"
                  placeholder="vps2.osi.my.id"
                  data-testid="host-input"
                />
              </div>
              <div>
                <Label htmlFor="port" className="text-xs font-medium text-slate-400 uppercase tracking-widest">Port</Label>
                <Input
                  id="port"
                  type="number"
                  value={connectionConfig.port}
                  onChange={(e) => setConnectionConfig({ ...connectionConfig, port: parseInt(e.target.value) })}
                  className="mt-2"
                  placeholder="6000"
                  data-testid="port-input"
                />
              </div>
              
              <div className="pt-4 border-t border-slate-800">
                <div className="flex items-center justify-between mb-2">
                  <Label className="text-xs font-medium text-slate-400 uppercase tracking-widest">Email Alert</Label>
                  <Badge variant="outline" className="text-xs">afif@ppns.ac.id</Badge>
                </div>
                <p className="text-xs text-slate-500 mb-3">
                  Email akan dikirim otomatis jika koneksi AIS terputus
                </p>
                <Button
                  onClick={handleTestAlert}
                  variant="outline"
                  className="w-full"
                  data-testid="test-alert-btn"
                >
                  Test Email Alert
                </Button>
              </div>
              
              <Button
                onClick={handleSaveConnection}
                className="w-full mt-6"
                data-testid="save-connection-btn"
              >
                Save Configuration
              </Button>
            </div>
          </Card>
        </div>
      )}

      {/* Vessel Detail Panel */}
      {showVesselDetail && selectedVessel && (
        <div className="vessel-detail-panel" data-testid="vessel-detail-panel">
          <div className="vessel-detail-header">
            <div>
              <h2 className="text-xl font-bold uppercase tracking-wider text-slate-100">
                {selectedVessel.name || `Vessel ${selectedVessel.mmsi}`}
              </h2>
              <div className="text-xs font-mono text-cyan-400 mt-1">MMSI: {selectedVessel.mmsi}</div>
            </div>
            <button
              onClick={() => setShowVesselDetail(false)}
              className="p-2 hover:bg-slate-800 rounded transition-colors"
              data-testid="close-detail-btn"
            >
              <X className="w-5 h-5 text-slate-400" />
            </button>
          </div>

          <div className="vessel-detail-content">
            {/* Position & Navigation */}
            <div className="detail-section">
              <h3 className="detail-section-title">Position & Navigation</h3>
              <div className="detail-grid">
                <div className="detail-item">
                  <span className="detail-label">Latitude</span>
                  <span className="detail-value">{selectedVessel.lat?.toFixed(4) || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Longitude</span>
                  <span className="detail-value">{selectedVessel.lon?.toFixed(4) || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Speed (SOG)</span>
                  <span className="detail-value">{selectedVessel.speed !== null ? `${selectedVessel.speed.toFixed(1)} kn` : 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Course (COG)</span>
                  <span className="detail-value">{selectedVessel.course !== null ? `${selectedVessel.course.toFixed(0)}°` : 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Heading</span>
                  <span className="detail-value">{selectedVessel.heading !== null ? `${selectedVessel.heading}°` : 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Status</span>
                  <span className="detail-value">{selectedVessel.status_text || 'N/A'}</span>
                </div>
              </div>
            </div>

            {/* Vessel Information */}
            <div className="detail-section">
              <h3 className="detail-section-title">Vessel Information</h3>
              <div className="detail-grid">
                <div className="detail-item">
                  <span className="detail-label">Call Sign</span>
                  <span className="detail-value">{selectedVessel.callsign || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Ship Type</span>
                  <span className="detail-value">{selectedVessel.ship_type_text || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">IMO</span>
                  <span className="detail-value">{selectedVessel.imo || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Destination</span>
                  <span className="detail-value">{selectedVessel.destination || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">ETA</span>
                  <span className="detail-value">{selectedVessel.eta || 'N/A'}</span>
                </div>
                <div className="detail-item">
                  <span className="detail-label">Draught</span>
                  <span className="detail-value">{selectedVessel.draught !== null ? `${selectedVessel.draught} m` : 'N/A'}</span>
                </div>
              </div>
            </div>

            {/* Dimensions */}
            {(selectedVessel.dim_bow || selectedVessel.dim_stern || selectedVessel.dim_port || selectedVessel.dim_starboard) && (
              <div className="detail-section">
                <h3 className="detail-section-title">Dimensions</h3>
                <div className="detail-grid">
                  <div className="detail-item">
                    <span className="detail-label">To Bow</span>
                    <span className="detail-value">{selectedVessel.dim_bow ? `${selectedVessel.dim_bow} m` : 'N/A'}</span>
                  </div>
                  <div className="detail-item">
                    <span className="detail-label">To Stern</span>
                    <span className="detail-value">{selectedVessel.dim_stern ? `${selectedVessel.dim_stern} m` : 'N/A'}</span>
                  </div>
                  <div className="detail-item">
                    <span className="detail-label">To Port</span>
                    <span className="detail-value">{selectedVessel.dim_port ? `${selectedVessel.dim_port} m` : 'N/A'}</span>
                  </div>
                  <div className="detail-item">
                    <span className="detail-label">To Starboard</span>
                    <span className="detail-value">{selectedVessel.dim_starboard ? `${selectedVessel.dim_starboard} m` : 'N/A'}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
