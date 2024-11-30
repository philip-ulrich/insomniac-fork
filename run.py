import sys
import os

# Add the current directory to Python path so it can find GramAddict
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
print(f"Python path: {sys.path}")
print(f"Current directory: {current_dir}")
print(f"Working directory: {os.getcwd()}")

try:
    import GramAddict
    print("Successfully imported GramAddict")
    GramAddict.run()
except Exception as e:
    print(f"Error running GramAddict: {str(e)}", file=sys.stderr)
    raise
