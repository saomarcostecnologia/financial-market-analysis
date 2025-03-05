class LoadStockDataUseCase:
    def __init__(self, repository):
        self.repository = repository

    def execute(self, data):
        # Persistir dados
        for item in data:
            self.repository.save(item)
        return len(data)
