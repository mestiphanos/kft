import logging
class Logger:
    """ A logger class for logging information on code"""
    def __init__(self,module_name,log_on = "both",level=logging.DEBUG,formats='%(asctime)s:%(levelname)s:%(message)s',) -> None:
        self.filename = "logs"+module_name + ".log"
        self.level = level
        self.formats = formats
        self.log_on = log_on

    def log(self):
        self.create_logger(self.filename)
        self.define_formatter()
        if self.log_on == "file":
            self.create_file_handler()
        elif self.log_on == "console":
            self.create_console_handler()
        elif self.log_on == "both":
            self.create_console_handler()
            self.create_file_handler()
        return self.logger

    def create_logger(self,module_name):
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(self.level)
    
    def define_formatter(self):
        self.formatter = logging.Formatter(self.formats)
        
    def create_console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)
      
    def create_file_handler(self):
        file_handler = logging.FileHandler(self.filename)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)