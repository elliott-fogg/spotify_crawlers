from crawler_base import SpotipyCrawlerBase
import os
import time
import json

class TopTracksCrawler(SpotipyCrawlerBase):

	def __init__(self, dirname, items_per_file=10000,
                 count_threshold=100, estimate_time=False):
		super().__init__(dirname, items_per_file, count_threshold, estimate_time)


	def initial_setup(self):
		print("Collecting all Artist IDs...")
		related_artists_results_filepath = os.path.join(self.data_folder, "related_artists.json")
		related_artists_data = json.load(open(related_artists_results_filepath, "r"))
		all_artist_ids = set(related_artists_data.keys())
		self.unsearched_items = all_artist_ids


	def get_items_to_search(self):
		for item in self.unsearched_items:
			return item


	def make_search_request(self, artist_id):
		results = self.sp.artist_top_tracks(artist_id)
		return results


	def process_search_results(self, artist_id, results):
		top_track_ids = []
		for track in results["tracks"]:
			top_track_ids.append(track["id"])
		self.searched_items[artist_id] = top_track_ids
		return [artist_id]



if __name__ == "__main__":
	TopTracksCrawler("top_tracks", 10000, 500, True)
