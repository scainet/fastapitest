from pydantic import BaseSettings

class Settings(BaseSettings):
    min_idle_time_autoclaim: int = 300000