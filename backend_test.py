#!/usr/bin/env python3
"""
AIS Vessel Monitoring Backend API Test Suite
Tests all backend endpoints and WebSocket functionality
"""

import requests
import json
import sys
import time
import asyncio
import websockets
from datetime import datetime
from typing import Dict, List, Any

class AISBackendTester:
    def __init__(self, base_url="https://vessel-tracker-26.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.ws_url = base_url.replace('https://', 'wss://').replace('http://', 'ws://') + "/api/ws"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []

    def log_test(self, name: str, success: bool, details: str = ""):
        """Log test result"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
            print(f"✅ {name}: PASSED {details}")
        else:
            print(f"❌ {name}: FAILED {details}")
        
        self.test_results.append({
            "test": name,
            "success": success,
            "details": details,
            "timestamp": datetime.now().isoformat()
        })

    def test_api_root(self) -> bool:
        """Test API root endpoint"""
        try:
            response = requests.get(f"{self.api_url}/", timeout=10)
            success = response.status_code == 200
            data = response.json() if success else {}
            
            self.log_test(
                "API Root", 
                success, 
                f"Status: {response.status_code}, Message: {data.get('message', 'N/A')}"
            )
            return success
        except Exception as e:
            self.log_test("API Root", False, f"Exception: {str(e)}")
            return False

    def test_connection_status(self) -> Dict[str, Any]:
        """Test connection status endpoint"""
        try:
            response = requests.get(f"{self.api_url}/connection/status", timeout=10)
            success = response.status_code == 200
            data = response.json() if success else {}
            
            if success:
                required_fields = ['active', 'host', 'port', 'vessel_count']
                missing_fields = [field for field in required_fields if field not in data]
                
                if missing_fields:
                    success = False
                    details = f"Missing fields: {missing_fields}"
                else:
                    details = f"Active: {data['active']}, Host: {data['host']}:{data['port']}, Vessels: {data['vessel_count']}"
            else:
                details = f"Status: {response.status_code}"
                data = {}
            
            self.log_test("Connection Status", success, details)
            return data
        except Exception as e:
            self.log_test("Connection Status", False, f"Exception: {str(e)}")
            return {}

    def test_connection_config(self) -> bool:
        """Test connection configuration endpoint"""
        try:
            # Test POST to update config
            config_data = {"host": "vps2.osi.my.id", "port": 6000}
            response = requests.post(
                f"{self.api_url}/connection/config", 
                json=config_data,
                timeout=10
            )
            success = response.status_code == 200
            data = response.json() if success else {}
            
            details = f"Status: {response.status_code}"
            if success and 'status' in data:
                details += f", Response: {data['status']}"
            
            self.log_test("Connection Config", success, details)
            return success
        except Exception as e:
            self.log_test("Connection Config", False, f"Exception: {str(e)}")
            return False

    def test_vessels_list(self) -> List[Dict[str, Any]]:
        """Test vessels list endpoint"""
        try:
            response = requests.get(f"{self.api_url}/vessels", timeout=15)
            success = response.status_code == 200
            vessels = response.json() if success else []
            
            if success:
                vessel_count = len(vessels)
                vessels_with_position = len([v for v in vessels if v.get('lat') and v.get('lon')])
                details = f"Total: {vessel_count}, With Position: {vessels_with_position}"
                
                # Check vessel data structure
                if vessels:
                    sample_vessel = vessels[0]
                    required_fields = ['mmsi', 'last_updated']
                    missing_fields = [field for field in required_fields if field not in sample_vessel]
                    if missing_fields:
                        details += f", Missing fields in sample: {missing_fields}"
            else:
                details = f"Status: {response.status_code}"
                vessels = []
            
            self.log_test("Vessels List", success, details)
            return vessels
        except Exception as e:
            self.log_test("Vessels List", False, f"Exception: {str(e)}")
            return []

    def test_vessel_detail(self, vessels: List[Dict[str, Any]]) -> bool:
        """Test individual vessel detail endpoint"""
        if not vessels:
            self.log_test("Vessel Detail", False, "No vessels available to test")
            return False
        
        try:
            # Test with first vessel
            test_vessel = vessels[0]
            mmsi = test_vessel['mmsi']
            
            response = requests.get(f"{self.api_url}/vessels/{mmsi}", timeout=10)
            success = response.status_code == 200
            data = response.json() if success else {}
            
            if success:
                details = f"MMSI: {mmsi}, Name: {data.get('name', 'N/A')}"
                if data.get('lat') and data.get('lon'):
                    details += f", Position: {data['lat']:.4f}, {data['lon']:.4f}"
            else:
                details = f"Status: {response.status_code}, MMSI: {mmsi}"
            
            self.log_test("Vessel Detail", success, details)
            return success
        except Exception as e:
            self.log_test("Vessel Detail", False, f"Exception: {str(e)}")
            return False

    def test_vessel_detail_invalid(self) -> bool:
        """Test vessel detail with invalid MMSI"""
        try:
            invalid_mmsi = 999999999
            response = requests.get(f"{self.api_url}/vessels/{invalid_mmsi}", timeout=10)
            success = response.status_code == 404
            
            details = f"Status: {response.status_code} (Expected 404)"
            self.log_test("Vessel Detail (Invalid)", success, details)
            return success
        except Exception as e:
            self.log_test("Vessel Detail (Invalid)", False, f"Exception: {str(e)}")
            return False

    async def test_websocket(self) -> bool:
        """Test WebSocket connection and data reception"""
        try:
            print(f"🔍 Testing WebSocket connection to {self.ws_url}...")
            
            async with websockets.connect(self.ws_url, timeout=10) as websocket:
                print("WebSocket connected, waiting for initial data...")
                
                # Wait for initial data message
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=15)
                    data = json.loads(message)
                    
                    if data.get('type') == 'initial_data':
                        vessels = data.get('data', [])
                        details = f"Received initial data with {len(vessels)} vessels"
                        success = True
                    else:
                        details = f"Unexpected message type: {data.get('type')}"
                        success = False
                    
                except asyncio.TimeoutError:
                    details = "Timeout waiting for initial data"
                    success = False
                
                # Try to receive one more message (vessel update)
                if success:
                    try:
                        print("Waiting for vessel update...")
                        message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        update_data = json.loads(message)
                        if update_data.get('type') == 'vessel_update':
                            details += f", Received vessel update for MMSI: {update_data.get('data', {}).get('mmsi')}"
                    except asyncio.TimeoutError:
                        details += ", No vessel updates received (may be normal)"
                
                self.log_test("WebSocket Connection", success, details)
                return success
                
        except Exception as e:
            self.log_test("WebSocket Connection", False, f"Exception: {str(e)}")
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all backend tests"""
        print("🚀 Starting AIS Backend API Tests")
        print(f"📡 Testing against: {self.base_url}")
        print("=" * 60)
        
        # Test API endpoints
        self.test_api_root()
        connection_status = self.test_connection_status()
        self.test_connection_config()
        vessels = self.test_vessels_list()
        self.test_vessel_detail(vessels)
        self.test_vessel_detail_invalid()
        
        # Test WebSocket
        print("\n🔌 Testing WebSocket Connection...")
        try:
            websocket_success = asyncio.run(self.test_websocket())
        except Exception as e:
            self.log_test("WebSocket Connection", False, f"Async error: {str(e)}")
            websocket_success = False
        
        # Summary
        print("\n" + "=" * 60)
        print(f"📊 Test Results: {self.tests_passed}/{self.tests_run} passed")
        
        success_rate = (self.tests_passed / self.tests_run * 100) if self.tests_run > 0 else 0
        print(f"✨ Success Rate: {success_rate:.1f}%")
        
        # Connection analysis
        if connection_status:
            print(f"\n🔗 AIS Connection Status:")
            print(f"   Active: {connection_status.get('active', 'Unknown')}")
            print(f"   Host: {connection_status.get('host', 'Unknown')}:{connection_status.get('port', 'Unknown')}")
            print(f"   Vessels Tracked: {connection_status.get('vessel_count', 0)}")
        
        # Vessel analysis
        if vessels:
            vessels_with_position = len([v for v in vessels if v.get('lat') and v.get('lon')])
            print(f"\n🚢 Vessel Data:")
            print(f"   Total Vessels: {len(vessels)}")
            print(f"   With GPS Position: {vessels_with_position}")
            print(f"   Position Coverage: {(vessels_with_position/len(vessels)*100):.1f}%")
        
        return {
            "tests_run": self.tests_run,
            "tests_passed": self.tests_passed,
            "success_rate": success_rate,
            "connection_status": connection_status,
            "vessel_count": len(vessels),
            "vessels_with_position": len([v for v in vessels if v.get('lat') and v.get('lon')]) if vessels else 0,
            "test_results": self.test_results
        }

def main():
    """Main test execution"""
    tester = AISBackendTester()
    results = tester.run_all_tests()
    
    # Return appropriate exit code
    if results["success_rate"] >= 80:
        print("\n🎉 Backend tests mostly successful!")
        return 0
    elif results["success_rate"] >= 50:
        print("\n⚠️  Backend has some issues but partially functional")
        return 1
    else:
        print("\n💥 Backend has major issues")
        return 2

if __name__ == "__main__":
    sys.exit(main())