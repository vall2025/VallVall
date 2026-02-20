from utility.library import *

def to_epoch(value, is_start=True):
    try:
        # Try to parse as 'YYYY-MM-DD HH:MM:SS'
        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            # Try to parse as 'YYYY-MM-DD'
            dt = datetime.strptime(value, '%Y-%m-%d')
            # Set time explicitly
            if is_start:
                dt = dt.replace(hour=0, minute=0, second=0)
            else:
                dt = dt.replace(hour=23, minute=59, second=59)
        except ValueError:
            raise ValueError("Invalid date format. Use 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.")
    return int(dt.timestamp())