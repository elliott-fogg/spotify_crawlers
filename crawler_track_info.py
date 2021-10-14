from crawler_base import SpotipyCrawlerBase
import os
import time
import json
from urllib3.exceptions import MaxRetryError

class TrackInfoCrawler(SpotipyCrawlerBase):

	def __init__(self, dirname, items_per_file=10000,
                 count_threshold=100, estimate_time=False):
		super().__init__(dirname, items_per_file, count_threshold, estimate_time)


	def initial_setup(self):
		print("Collecting all Track IDs...")
		top_tracks_filepath = os.path.join(self.data_folder, "top_tracks.json")
		top_tracks_data = json.load(open(top_tracks_filepath, "r"))
		all_track_ids = set()
		for artist_id in top_tracks_data:
			all_track_ids.update(top_tracks_data[artist_id])
		print(f"{len(all_track_ids)} Track IDs found.")
		self.unsearched_items = all_track_ids


	def get_items_to_search(self):
		items_to_search = []
		count = 0
		for item in self.unsearched_items:
			items_to_search.append(item)
			count += 1
			if count >= 50: # Spotify-imposed limit for sp.tracks()
				break
		return items_to_search


	def make_search_request(self, tracks_to_search):
		results = {}
		results["info"] = self.sp.tracks(tracks_to_search)
		try:
			print("Testing...")
			results["af"] = self.sp.audio_features(tracks_to_search)
		except MaxRetryError:
			print("Error 1")
			af_results = []
			for track_id in tracks_to_search:
				try:
					r = self.sp.audio_features([track_id])
					af_results += r
				except MaxRetryError:
					print("Error 2")
					af_results += [None]
			results["af"] = af_results

		return results


	def process_search_results(self, tracks_to_search, results):
		r_info = results["info"]
		r_af = results["af"]

		track_info = {}

		for i in range(len(tracks_to_search)):
			info = {}
			t_info = r_info["tracks"][i]
			t_af = r_af[i]

			artist_ids = []
			for artist in t_info["artists"]:
				artist_ids.append(artist["id"])
			info["artists"] = artist_ids

			info["duration_ms"] = t_info["duration_ms"]
			info["explicit"] = t_info["explicit"]
			info["id"] = t_info["id"]
			info["name"] = t_info["name"]
			info["popularity"] = t_info["popularity"]
			info["track_number"] = t_info["track_number"]
			info["release_date"] = t_info["album"]["release_date"]
			info["release_date_precision"] = t_info["album"]["release_date_precision"]
			info["album_name"] = t_info["album"]["name"]
			info["album_total_tracks"] = t_info["album"]["total_tracks"]
			info["available_markets"] = t_info["available_markets"]

			if t_af != None:
				info["danceability"] = t_af["danceability"]
				info["energy"] = t_af["energy"]
				info["key"] = t_af["key"]
				info["loudness"] = t_af["loudness"]
				info["mode"] = t_af["mode"]
				info["speechiness"] = t_af["speechiness"]
				info["acousticness"] = t_af["acousticness"]
				info["instrumentalness"] = t_af["instrumentalness"]
				info["liveness"] = t_af["liveness"]
				info["valence"] = t_af["valence"]
				info["tempo"] = t_af["tempo"]
				info["time_signature"] = t_af["time_signature"]
				info["duration_ms2"] = t_af["duration_ms"]

			else:
				info["danceability"] = None
				info["energy"] = None
				info["key"] = None
				info["loudness"] = None
				info["mode"] = None
				info["speechiness"] = None
				info["acousticness"] = None
				info["instrumentalness"] = None
				info["liveness"] = None
				info["valence"] = None
				info["tempo"] = None
				info["time_signature"] = None
				info["duration_ms2"] = None

			track_info[tracks_to_search[i]] = info

		self.searched_items.update(track_info)
		return tracks_to_search



if __name__ == "__main__":
	TrackInfoCrawler("track_info", 10000, 1000, True)
