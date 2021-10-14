from crawler_base import SpotipyCrawlerBase
import os
import time
import json

class ArtistInfoCrawler(SpotipyCrawlerBase):

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
		items_to_search = []
		count = 0
		for item in self.unsearched_items:
			items_to_search.append(item)
			count += 1
			if count >= 50: # Spotify-imposed limit
				break
		return items_to_search


	def make_search_request(self, artists_to_search):
		results = self.sp.artists(artists_to_search)
		return results


	def process_search_results(self, artists_to_search, results):
		for i in range(len(artists_to_search)):
			original_id = artists_to_search[i]
			artist_info = results["artists"][i]
			self.searched_items[original_id] = {
				"id": artist_info["id"],
        		"name": artist_info["name"],
        		"followers": artist_info["followers"]["total"],
        		"popularity": artist_info["popularity"],
        		"genres": artist_info["genres"]
        	}
        return artists_to_search


	# def search_items(self, artists_to_search):
	# 	sleep_times = [0, 0.01, 0.1, 1, 5, 10]
	# 	success = False

	# 	for t in sleep_times:
	# 		time.sleep(t)
	# 		try:
	# 			response = self.sp.artists(artists_to_search)
	# 			success = True
	# 			break
            
	# 		except (WantReadError, ReadTimeout):
	# 			continue
            
	# 	if not success:
	# 		print("\n{}".format(artists_to_search))
	# 		raise AssertionError("Could not have request processed even after 10s wait.")

	# 	for i in range(len(artists_to_search)):
	# 		original_id = artists_to_search[i]
	# 		artist_info = response["artists"][i]
	# 		self.searched_items[original_id] = {
 #        		"id": artist_info["id"],
 #        		"name": artist_info["name"],
 #        		"followers": artist_info["followers"]["total"],
 #        		"popularity": artist_info["popularity"],
 #        		"genres": artist_info["genres"]
 #        	}
	# 	return artists_to_search



if __name__ == "__main__":
	ArtistInfoCrawler("artist_info", 10000, 100, False)
