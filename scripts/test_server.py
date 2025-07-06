import requests
def test_server_connection(base_url='http://localhost:24601'):
    """Test if the server is accessible"""
    try:
        response = requests.get(f"{base_url}/", timeout=10)
        print(f"✓ Server is running - Status: {response.status_code}")
        return True
    except requests.exceptions.ConnectionError:
        print("✗ Server is not running or connection refused")
        return False
    except requests.exceptions.Timeout:
        print("✗ Server timeout - server may be overloaded")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

# # Test before uploading
# if test_server_connection():
#     upload_all_chunks_with_history_merge()
# else:
#     print("Please start the server first")
test_server_connection()