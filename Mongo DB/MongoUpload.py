class MongoUpload:
    mongoDB_url = 4123498

    def __init__(self, collection):
        """Upload Collection Object to MongoDB"""

    def __repr__(self):
        return 'MongoDB connection at {}'.format(self.mongoDB_url)