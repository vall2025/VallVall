from utility.library import *

def get_device_id():
    system_platform = platform.system()

    try:
        if system_platform == "Windows":
            result = subprocess.check_output("wmic bios get serialnumber", shell=True)
            serial = result.decode().split("\n")[1].strip()
            return serial if serial else hex(uuid.getnode())  # Fallback to MAC address if serial is empty

        elif system_platform == "Linux":
            # For Ubuntu and other Linux distributions
            if os.path.exists("/etc/machine-id"):
                with open("/etc/machine-id", "r") as f:
                    return f.read().strip()
            elif os.path.exists("/var/lib/dbus/machine-id"):
                with open("/var/lib/dbus/machine-id", "r") as f:
                    return f.read().strip()
            else:
                return hex(uuid.getnode())  # Fallback to MAC address

        elif system_platform == "Darwin":
            result = subprocess.check_output(
                "ioreg -rd1 -c IOPlatformExpertDevice | grep IOPlatformUUID", shell=True
            )
            return result.decode().split('"')[-2]

        elif system_platform == "Android":
            # Android devices typically use the `android.os.Build.SERIAL` (deprecated in API 29), but here’s a generic fallback method
            try:
                serial = subprocess.check_output("adb shell settings get secure android_id", shell=True).decode().strip()
                if serial:
                    return serial
            except subprocess.CalledProcessError as e:
                return hex(uuid.getnode())  # Fallback to MAC address if adb fails

        else:
            return hex(uuid.getnode())  # Fallback to MAC address if adb fails

    except Exception as e:
        return hex(uuid.getnode())