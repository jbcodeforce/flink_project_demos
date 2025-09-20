#!/usr/bin/env python3
"""
Customer Analytics C360 API - Setup and Installation Script
Helps set up the development environment and dependencies
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Error: Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        sys.exit(1)
    print(f"✅ Python version: {sys.version.split()[0]}")


def check_command_exists(command):
    """Check if a command exists in the system PATH"""
    return shutil.which(command) is not None


def check_dependencies():
    """Check if required system dependencies are available"""
    dependencies = {
        "spark-sql": "Apache Spark (install with: brew install apache-spark)",
        "java": "Java Runtime Environment (install with: brew install openjdk)",
    }
    
    missing_deps = []
    for cmd, description in dependencies.items():
        if check_command_exists(cmd):
            print(f"✅ {cmd} is available")
        else:
            print(f"❌ {cmd} is missing - {description}")
            missing_deps.append(cmd)
    
    if missing_deps:
        print(f"\n❌ Missing dependencies: {', '.join(missing_deps)}")
        print("Please install the missing dependencies before continuing.")
        return False
    
    return True


def check_uv_installation():
    """Check if uv is installed, install if not"""
    if check_command_exists("uv"):
        print("✅ uv package manager is available")
        return True
    
    print("📦 uv not found, installing...")
    try:
        # Install uv using the official installer
        subprocess.run([
            sys.executable, "-c", 
            "import urllib.request; exec(urllib.request.urlopen('https://astral.sh/uv/install.py').read())"
        ], check=True)
        
        # Check if uv is now available
        if check_command_exists("uv"):
            print("✅ uv installed successfully")
            return True
        else:
            print("❌ uv installation failed - please install manually: curl -LsSf https://astral.sh/uv/install.sh | sh")
            return False
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install uv: {e}")
        print("💡 Manual installation: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return False


def install_python_dependencies():
    """Install Python dependencies using uv"""
    print("\n📦 Installing Python dependencies with uv...")
    
    # Check/install uv first
    if not check_uv_installation():
        return False
    
    try:
        # Install dependencies using uv
        subprocess.run(["uv", "pip", "install", "-e", ".", "--system"], check=True)
        print("✅ Python dependencies installed successfully with uv")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install Python dependencies with uv: {e}")
        print("💡 Trying fallback installation...")
        
        # Fallback to direct uv sync
        try:
            subprocess.run(["uv", "sync"], check=True)
            print("✅ Dependencies installed with uv sync")
            return True
        except subprocess.CalledProcessError as e2:
            print(f"❌ Fallback installation also failed: {e2}")
            return False


def verify_c360_pipeline():
    """Verify that the C360 pipeline is available"""
    pipeline_path = Path("../c360_spark_processing")
    pipeline_file = pipeline_path / "c360_consolidated_pipeline.sql"
    data_path = Path("../c360_mock_data")
    
    if not pipeline_path.exists():
        print(f"❌ Pipeline directory not found: {pipeline_path}")
        return False
    
    if not pipeline_file.exists():
        print(f"❌ Pipeline file not found: {pipeline_file}")
        return False
    
    if not data_path.exists():
        print(f"❌ Mock data directory not found: {data_path}")
        return False
    
    print("✅ C360 pipeline and data are available")
    return True


def create_env_file():
    """Create a sample .env file if it doesn't exist"""
    env_file = Path(".env")
    if env_file.exists():
        print("✅ .env file already exists")
        return
    
    env_content = """# Customer Analytics C360 API Configuration

# Environment
ENVIRONMENT=development
DEBUG=true

# API Settings
API_HOST=0.0.0.0
API_PORT=8000

# Data Pipeline Settings
C360_DATA_PATH=../c360_mock_data
PIPELINE_PATH=../c360_spark_processing
CACHE_TTL_MINUTES=30

# Spark Settings
SPARK_APP_NAME=C360_API
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Security (Change in production!)
SECRET_KEY=your-secret-key-change-in-production

# Rate Limiting
RATE_LIMIT_PER_MINUTE=100

# CORS Settings (Adjust for production)
CORS_ORIGINS=*
CORS_METHODS=*
CORS_HEADERS=*
"""
    
    with open(env_file, "w") as f:
        f.write(env_content)
    
    print("✅ Created sample .env file")


def test_api_startup():
    """Test if the API can start up successfully"""
    print("\n🧪 Testing API startup...")
    try:
        # Import main modules to check for import errors
        import fastapi
        import pydantic
        print("✅ FastAPI and Pydantic imports successful")
        
        # Try importing our modules
        from models import CustomerProfile, APIResponse
        from config import settings
        print("✅ Application modules import successfully")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Try running: uv sync")
        return False


def run_pipeline_test():
    """Test running the C360 pipeline"""
    print("\n🔄 Testing C360 pipeline execution...")
    try:
        os.chdir("../c360_spark_processing")
        result = subprocess.run([
            "spark-sql", 
            "-e", "SELECT 'Pipeline test successful' as test;",
            "--silent"
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Spark SQL execution test successful")
            return True
        else:
            print(f"❌ Spark SQL test failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Spark SQL test timed out")
        return False
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        return False
    finally:
        os.chdir("../c360_api")


def main():
    """Main setup function"""
    print("🚀 Customer Analytics C360 API Setup")
    print("=====================================\n")
    
    # Check Python version
    check_python_version()
    
    # Check system dependencies
    if not check_dependencies():
        print("\n❌ Setup failed due to missing system dependencies")
        sys.exit(1)
    
    # Install Python dependencies
    if not install_python_dependencies():
        print("\n❌ Setup failed during Python dependency installation")
        sys.exit(1)
    
    # Verify C360 pipeline
    if not verify_c360_pipeline():
        print("\n❌ Setup failed: C360 pipeline not available")
        print("   Please ensure you're running this from the c360_api directory")
        print("   and that the c360_spark_processing and c360_mock_data directories exist")
        sys.exit(1)
    
    # Create environment file
    create_env_file()
    
    # Test API imports
    if not test_api_startup():
        print("\n❌ Setup failed during API startup test")
        sys.exit(1)
    
    # Test pipeline execution
    if not run_pipeline_test():
        print("\n⚠️  Warning: Pipeline test failed")
        print("   The API may still work, but data refresh might not function properly")
        print("   Please check your Spark installation and C360 pipeline")
    
    print("\n🎉 Setup completed successfully!")
    print("\n📚 Next steps:")
    print("   1. Review the .env file and adjust settings as needed")
    print("   2. Start the API with: uv run python main.py")
    print("   3. Visit http://localhost:8000/docs for API documentation")
    print("   4. Test with: curl http://localhost:8000/health")
    print("\n💡 Useful uv commands:")
    print("   - Install dependencies: uv sync")
    print("   - Start API: uv run python main.py")
    print("   - Start with auto-reload: uv run uvicorn main:app --reload")
    print("   - Run tests: uv run python test_api.py")
    print("   - Add dependency: uv add <package-name>")
    print("   - Add dev dependency: uv add --dev <package-name>")


if __name__ == "__main__":
    main()
