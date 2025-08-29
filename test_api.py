#!/usr/bin/env python3
"""
Test script for IMDb API
Tests all endpoints and validates responses
"""

import requests
import json
import sys
import time
from typing import Dict, Any

class APITester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.tests_passed = 0
        self.tests_failed = 0
    
    def log(self, message: str, level: str = "INFO"):
        colors = {
            "INFO": "\033[94m",
            "SUCCESS": "\033[92m", 
            "ERROR": "\033[91m",
            "WARNING": "\033[93m",
            "END": "\033[0m"
        }
        print(f"{colors.get(level, '')}{level}: {message}{colors['END']}")
    
    def make_request(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make API request and return response"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=30)
            return {
                "status_code": response.status_code,
                "data": response.json() if response.content else {},
                "success": response.status_code == 200
            }
        except Exception as e:
            return {
                "status_code": 0,
                "data": {"error": str(e)},
                "success": False
            }
    
    def test_endpoint(self, name: str, endpoint: str, params: Dict = None, 
                     expected_fields: list = None) -> bool:
        """Test a single endpoint"""
        self.log(f"Testing {name}...")
        
        result = self.make_request(endpoint, params)
        
        if not result["success"]:
            self.log(f"❌ {name} failed: {result['data'].get('error', 'Unknown error')}", "ERROR")
            self.tests_failed += 1
            return False
        
        # Check expected fields if provided
        if expected_fields:
            for field in expected_fields:
                if field not in result["data"]:
                    self.log(f"❌ {name} missing field: {field}", "ERROR")
                    self.tests_failed += 1
                    return False
        
        self.log(f"✅ {name} passed", "SUCCESS")
        self.tests_passed += 1
        return True
    
    def test_health(self) -> bool:
        """Test health endpoint"""
        return self.test_endpoint(
            "Health Check",
            "/health",
            expected_fields=["status", "timestamp"]
        )
    
    def test_root(self) -> bool:
        """Test root endpoint"""
        return self.test_endpoint(
            "Root Endpoint",
            "/",
            expected_fields=["message", "version", "endpoints"]
        )
    
    def test_status(self) -> bool:
        """Test ETL status endpoint"""
        return self.test_endpoint(
            "ETL Status",
            "/status",
            expected_fields=["status"]
        )
    
    def test_actors(self) -> bool:
        """Test actors endpoint with various parameters"""
        tests = [
            ("Actors (default)", {"profession": "actor"}),
            ("Actresses (default)", {"profession": "actress"}),
            ("Actors (limit 5)", {"profession": "actor", "limit": 5}),
            ("Actors (with offset)", {"profession": "actor", "limit": 10, "offset": 10}),
        ]
        
        all_passed = True
        expected_fields = ["actors", "total", "limit", "offset", "profession"]
        
        for test_name, params in tests:
            result = self.test_endpoint(f"Actors - {test_name}", "/actors", params, expected_fields)
            if result:
                # Additional validation for actors response
                response = self.make_request("/actors", params)
                actors = response["data"].get("actors", [])
                
                if not actors:
                    self.log(f"⚠️  {test_name} returned no actors", "WARNING")
                else:
                    # Check actor object structure
                    actor = actors[0]
                    required_actor_fields = ["name", "score", "number_of_titles", "total_runtime_minutes"]
                    
                    for field in required_actor_fields:
                        if field not in actor:
                            self.log(f"❌ Actor object missing field: {field}", "ERROR")
                            all_passed = False
                            break
                    
                    # Validate data types
                    if isinstance(actor.get("score"), (int, float)) and actor["score"] >= 0:
                        self.log(f"✅ Score validation passed: {actor['score']}", "SUCCESS")
                    else:
                        self.log(f"❌ Invalid score: {actor.get('score')}", "ERROR")
                        all_passed = False
            else:
                all_passed = False
        
        return all_passed
    
    def test_error_cases(self) -> bool:
        """Test error handling"""
        tests = [
            ("Invalid profession", "/actors", {"profession": "director"}),
            ("Negative limit", "/actors", {"profession": "actor", "limit": -1}),
            ("Excessive limit", "/actors", {"profession": "actor", "limit": 10000}),
        ]
        
        all_passed = True
        
        for test_name, endpoint, params in tests:
            self.log(f"Testing error case: {test_name}...")
            result = self.make_request(endpoint, params)
            
            if result["status_code"] in [400, 422]:  # Bad request or validation error
                self.log(f"✅ {test_name} correctly returned error", "SUCCESS")
                self.tests_passed += 1
            else:
                self.log(f"❌ {test_name} should have returned error but got: {result['status_code']}", "ERROR")
                self.tests_failed += 1
                all_passed = False
        
        return all_passed
    
    def test_performance(self) -> bool:
        """Basic performance test"""
        self.log("Testing response times...")
        
        start_time = time.time()
        result = self.make_request("/actors", {"profession": "actor", "limit": 100})
        end_time = time.time()
        
        response_time = end_time - start_time
        
        if result["success"]:
            if response_time < 5.0:  # Should respond within 5 seconds
                self.log(f"✅ Performance test passed: {response_time:.2f}s", "SUCCESS")
                self.tests_passed += 1
                return True
            else:
                self.log(f"⚠️  Slow response: {response_time:.2f}s", "WARNING")
                self.tests_passed += 1
                return True
        else:
            self.log(f"❌ Performance test failed: request failed", "ERROR")
            self.tests_failed += 1
            return False
    
    def wait_for_api(self, max_attempts: int = 20) -> bool:
        """Wait for API to be available"""
        self.log("Waiting for API to be available...")
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=5)
                if response.status_code == 200:
                    self.log("API is available", "SUCCESS")
                    return True
            except:
                pass
            
            if attempt < max_attempts - 1:
                self.log(f"Attempt {attempt + 1}/{max_attempts} - waiting...")
                time.sleep(3)
        
        self.log("API is not available after waiting", "ERROR")
        return False
    
    def run_all_tests(self) -> bool:
        """Run all tests"""
        self.log("🎬 Starting IMDb API Tests", "INFO")
        print("=" * 50)
        
        # Wait for API to be available
        if not self.wait_for_api():
            return False
        
        # Run tests
        tests = [
            self.test_health,
            self.test_root,
            self.test_status,
            self.test_actors,
            self.test_error_cases,
            self.test_performance,
        ]
        
        for test in tests:
            test()
            print()  # Add spacing between tests
        
        # Print summary
        print("=" * 50)
        total_tests = self.tests_passed + self.tests_failed
        self.log(f"Tests Summary: {self.tests_passed}/{total_tests} passed", "INFO")
        
        if self.tests_failed == 0:
            self.log("🎉 All tests passed!", "SUCCESS")
            return True
        else:
            self.log(f"❌ {self.tests_failed} tests failed", "ERROR")
            return False

def main():
    """Main function"""
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    
    tester = APITester(base_url)
    success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()