import asyncio
import sys
import logging
import signal
from datetime import datetime
from asyncio.subprocess import Process
from typing import Dict, List
import colorama
from colorama import Fore, Style

# Initialize colorama for Windows
colorama.init()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("supervisor")


class ComponentSupervisor:
    def __init__(self):
        self.processes: Dict[str, Process] = {}
        self.should_run = True
        self.log_tasks: List[asyncio.Task] = []

        # Component colors for logs
        self.colors = {
            "github_producer": Fore.GREEN,
            "postgres_consumer": Fore.BLUE,
            "minio_consumer": Fore.YELLOW,
            "drift_detector": Fore.MAGENTA,
        }

    async def start_component(self, name: str, module_path: str):
        """Start a component and capture its output."""
        try:
            # Create the process
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", module_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            self.processes[name] = process

            # Create tasks for reading stdout and stderr
            self.log_tasks.extend([
                asyncio.create_task(self.log_output(
                    name, process.stdout, "INFO")),
                asyncio.create_task(self.log_output(
                    name, process.stderr, "ERROR"))
            ])

            logger.info(f"Started {name} (PID: {process.pid})")

        except Exception as e:
            logger.error(f"Failed to start {name}: {e}")

    async def log_output(self, name: str, pipe, level: str):
        """Read and log output from a process pipe."""
        color = self.colors.get(name, "")
        while self.should_run:
            try:
                line = await pipe.readline()
                if not line:
                    break

                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                message = line.decode().strip()

                # Format: [TIMESTAMP] COMPONENT_NAME: MESSAGE
                print(
                    f"{Fore.WHITE}[{timestamp}] {color}{name}: {message}{Style.RESET_ALL}")

            except Exception as e:
                logger.error(f"Error reading {name} output: {e}")
                break

    async def start_all(self):
        """Start all components."""
        components = {
            "github_producer": "ingestion.producers.github_producer",
            "postgres_consumer": "ingestion.consumers.postgres_consumer",
            "minio_consumer": "ingestion.consumers.minio_consumer",
            "drift_detector": "drift_engine.drift_service"
        }

        print(f"{Fore.CYAN}Starting all components...{Style.RESET_ALL}")

        for name, module in components.items():
            await self.start_component(name, module)

    async def shutdown(self):
        """Gracefully shutdown all components."""
        print(f"\n{Fore.YELLOW}Shutting down all components...{Style.RESET_ALL}")
        self.should_run = False

        # Terminate all processes
        for name, process in self.processes.items():
            try:
                process.terminate()
                await process.wait()
                print(f"{Fore.GREEN}Successfully stopped {name}{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error stopping {name}: {e}{Style.RESET_ALL}")

        # Cancel all log tasks
        for task in self.log_tasks:
            task.cancel()

        print(f"{Fore.CYAN}Shutdown complete{Style.RESET_ALL}")


async def main():
    supervisor = ComponentSupervisor()

    try:
        await supervisor.start_all()
        # Keep the script running
        while supervisor.should_run:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
    finally:
        await supervisor.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Received shutdown signal{Style.RESET_ALL}")
