from crawler_base import SpotipyCrawlerBase

class RelatedArtistsCrawler(SpotipyCrawlerBase):

	def __init__(self, dirname, items_per_file=10000,
                 count_threshold=100, estimate_time=False):
		super().__init__(dirname, items_per_file, count_threshold, estimate_time)


	def initial_setup(self):
		self.unsearched_items.add("4iHNK0tOyZPYnBU7nGAgpQ") # Add seed artist


	def get_items_to_search(self):
		for item in self.unsearched_items:
			return item


	def make_search_request(self, artist_id):
		results = self.sp.artist_related_artists(artist_id)
		return results


	def process_search_results(self, artist_id, results):
		new_ids = [artist["id"] for artist in related_artists["artists"]]
		for new_id in new_ids:
			if (new_id not in self.saved_items) and \
											(new_id not in self.searched_items):
				self.unsearched_items.add(new_id)
		self.searched_items[artist_id] = new_ids
		# self.clear_searched_items takes in a list, so return ID as a list.
		return [artist_id]


	# def search_items(self, artist_id):
	# 	related_artists = self.sp.artist_related_artists(artist_id)
	# 	new_ids = [artist["id"] for artist in related_artists["artists"]]
	# 	for new_id in new_ids:
	# 		if (new_id not in self.saved_items) and \
	# 			(new_id not in self.searched_items):
	# 			self.unsearched_items.add(new_id)
	# 	self.searched_items[artist_id] = new_ids
	# 	# self.clear_searched_items takes in a list, so return ID as a list.
	# 	return [artist_id] 



if __name__ == "__main__":
	RelatedArtistsCrawler("related_artists", 10000, 100, False)
