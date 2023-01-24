from enum import Enum


class CrawlerState(Enum):
    phase_create_links = 'created links'
    phase_download_links = 'downloaded links'
    phase_pars_data = 'parsed data'
    phase_save_data = 'saved data'
