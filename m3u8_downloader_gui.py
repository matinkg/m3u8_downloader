# m3u8_downloader_gui.py

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import requests
import m3u8
import os
import json
import threading
import queue
import subprocess
import shutil
import time
from urllib.parse import urljoin, urlparse
import concurrent.futures  # Ensure this is imported
import traceback  # For printing full tracebacks

# --- Configuration ---
DEFAULT_RESOLUTION = "720"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
FFMPEG_PATH = "ffmpeg"  # Assumes ffmpeg is in PATH. Change if necessary.

# --- Helper Functions ---


def is_ffmpeg_installed():
    """Checks if ffmpeg is accessible."""
    return shutil.which(FFMPEG_PATH) is not None


def sanitize_filename(name):
    """Removes or replaces characters invalid for filenames."""
    name = str(name)  # Ensure it's a string
    # Remove characters that are definitely invalid on most systems
    name = name.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-').replace(
        '?', '-').replace('"', "'").replace('<', '-').replace('>', '-').replace('|', '-')
    # Remove leading/trailing whitespace and control characters
    name = "".join(c for c in name if c.isprintable())
    name = name.strip()
    # Replace multiple spaces with single space
    name = ' '.join(name.split())
    # Limit length if necessary (optional, uncomment if needed)
    # max_len = 100
    # if len(name) > max_len:
    #     name = name[:max_len].rsplit(' ', 1)[0] # try to cut at a space
    return name if name else "downloaded_video"


def get_video_and_audio_playlists(manifest, preferred_res=None):
    """
    Selects video playlist, identifies primary audio, and lists all associated audio.

    Returns: (
        video_playlist_object or None,
        primary_audio_info or None, # {'uri': str, 'lang': str, 'name': str, 'default': bool}
        all_audio_infos or []      # List of audio_info dicts for the group
    )
    """
    target_playlist = None
    best_bandwidth = -1
    all_audio_infos = []
    primary_audio_info = None

    # --- Find Best Video Playlist (Similar to previous get_best_playlist) ---
    if not manifest.playlists:  # Handle direct media playlist input
        if manifest.segments:
            print("Input seems to be a media playlist (no separate audio/video streams).")
            return manifest, None, []  # Assume muxed, no separate audio info
        else:
            print("Error: No playlists found and no segments found.")
            return None, None, []

    # Select video based on preference or best bandwidth
    preferred_found = False
    if preferred_res:
        try:
            height_to_find = int(preferred_res)
            for playlist in manifest.playlists:
                # Check if resolution attribute exists and is not None
                if playlist.stream_info.resolution and len(playlist.stream_info.resolution) > 1:
                    if playlist.stream_info.resolution[1] == height_to_find:
                        target_playlist = playlist
                        print(f"Found preferred resolution: {preferred_res}p")
                        preferred_found = True
                        break
        except ValueError:
            print(f"Warning: Invalid preferred resolution '{preferred_res}'.")
        except Exception as e:
            print(f"Warning: Error checking resolution: {e}")

    # If preferred not found or not specified, find the best available (highest bandwidth)
    if not preferred_found:
        print(
            f"Preferred resolution {preferred_res}p not found/specified. Selecting best available.")
        for playlist in manifest.playlists:
            bandwidth = getattr(playlist.stream_info, 'bandwidth', -1)
            if bandwidth is None:
                bandwidth = -1
            current_height = playlist.stream_info.resolution[1] if (
                playlist.stream_info.resolution and len(playlist.stream_info.resolution) > 1) else 0
            best_height = target_playlist.stream_info.resolution[1] if (
                target_playlist and target_playlist.stream_info.resolution and len(target_playlist.stream_info.resolution) > 1) else 0

            # Select based on bandwidth primarily, then resolution as tie-breaker
            if bandwidth > best_bandwidth:
                best_bandwidth = bandwidth
                target_playlist = playlist
            elif bandwidth == best_bandwidth and current_height > best_height:
                target_playlist = playlist  # Higher resolution for same bandwidth

    if not target_playlist and manifest.playlists:
        print("Warning: Could not determine best video playlist. Picking first.")
        target_playlist = manifest.playlists[0]
    elif not target_playlist:
        print("Error: Could not determine any video playlist.")
        return None, None, []

    # --- Find Associated Audio Tracks ---
    selected_audio_group_id = getattr(
        target_playlist.stream_info, 'audio', None)
    if selected_audio_group_id:
        print(
            f"Video stream associated with audio group: {selected_audio_group_id}")
        found_audios = []
        for media in manifest.media:
            if media.type == 'AUDIO' and media.group_id == selected_audio_group_id and media.uri:
                audio_info = {
                    'uri': urljoin(manifest.base_uri, media.uri),
                    'lang': getattr(media, 'language', None),
                    'name': getattr(media, 'name', None),
                    'default': getattr(media, 'default', False)
                }
                found_audios.append(audio_info)
                # Identify primary audio (prefer 'default', fallback to first found)
                if audio_info['default']:
                    primary_audio_info = audio_info
                elif not primary_audio_info:  # If default not found yet, take the first one
                    primary_audio_info = audio_info

        if not found_audios:
            print(
                f"Audio group '{selected_audio_group_id}' mentioned but no matching AUDIO media tags with URI found.")
        else:
            all_audio_infos = found_audios
            if primary_audio_info:
                print(
                    f"Primary audio track selected: Lang={primary_audio_info['lang']}, Name={primary_audio_info['name']}")
            else:
                # This case shouldn't happen if found_audios is not empty, but safeguard
                print(
                    "Warning: Found audio tracks but couldn't determine primary. Using first.")
                if all_audio_infos:  # Ensure list is not empty
                    primary_audio_info = all_audio_infos[0]

            print(
                f"Found {len(all_audio_infos)} total audio tracks for group {selected_audio_group_id}.")

    else:
        print("No separate audio group specified for the selected video stream. Assuming muxed audio.")

    # Print selected video info
    res_info = "Unknown Resolution"
    if target_playlist.stream_info.resolution and len(target_playlist.stream_info.resolution) > 1:
        res_info = f"{target_playlist.stream_info.resolution[1]}p"
    bw_info = f"Bandwidth: {getattr(target_playlist.stream_info, 'bandwidth', 'N/A')}"
    print(f"Selected video stream: {res_info} ({bw_info})")

    return target_playlist, primary_audio_info, all_audio_infos


def get_all_subtitle_playlists(manifest):
    """ Finds all available subtitle tracks with URIs. """
    subtitle_infos = []
    if not manifest.media:
        return []

    print("Searching for subtitle tracks...")
    count = 0
    for media in manifest.media:
        if media.type == 'SUBTITLES' and media.uri:  # Ensure URI exists
            sub_info = {
                'uri': urljoin(manifest.base_uri, media.uri),
                'lang': getattr(media, 'language', None),
                'name': getattr(media, 'name', None)
            }
            subtitle_infos.append(sub_info)
            count += 1
            print(
                f"  Found subtitle: Lang={sub_info['lang']}, Name={sub_info['name']}, URI={sub_info['uri']}")

    print(f"Found {count} subtitle track(s) in total.")
    return subtitle_infos


# --- Download Logic ---

class Downloader:
    def __init__(self, url, output_dir, filename_base, preferred_res, download_subs, gui_queue):
        self.m3u8_url = url
        # Base output directory (e.g., Downloads)
        self.base_output_dir = output_dir
        # Sanitized name for folder and base filenames
        self.filename_base = sanitize_filename(filename_base)
        # Specific directory for this download item
        self.item_output_dir = os.path.join(
            self.base_output_dir, self.filename_base)
        self.preferred_res = preferred_res
        self.download_subs = download_subs
        self.gui_queue = gui_queue
        self.stop_event = threading.Event()
        self.item_id = None  # Will be set by the GUI

    def _update_status(self, status, progress=None):
        """ Safely send status update to the GUI queue. """
        # Avoid sending updates if stop event is already set, reduces queue noise during stopping
        if self.stop_event.is_set() and status not in ["Stopping...", "Stopped", "FINISHED"]:
            return
        try:
            update = {"id": self.item_id, "status": status}
            if progress is not None:
                # Ensure progress is between 0 and 100
                update["progress"] = max(0, min(100, int(progress)))
            self.gui_queue.put(update)
        except Exception as e:
            print(f"Error updating status via queue: {e}")

    def _download_segment(self, segment_uri, temp_dir, index, total_segments, session):
        """ Downloads a single segment file with retries. """
        if self.stop_event.is_set():
            return False  # Stop requested

        # Ensure the temp directory exists just before writing
        os.makedirs(temp_dir, exist_ok=True)
        # Determine expected extension (usually .ts or .aac for audio only)
        # For simplicity, we assume .ts for video/primary audio segments downloaded this way
        segment_filename = os.path.join(temp_dir, f"segment_{index:05d}.ts")

        attempts = 3
        for attempt in range(attempts):
            if self.stop_event.is_set():
                return False
            try:
                headers = {'User-Agent': USER_AGENT}
                # Increased timeout for potentially larger segments or slow connections
                response = session.get(segment_uri, stream=True, timeout=(
                    # (connect_timeout, read_timeout)
                    10, 30), headers=headers)
                response.raise_for_status()

                with open(segment_filename, 'wb') as f:
                    # Slightly larger chunk size
                    for chunk in response.iter_content(chunk_size=8192 * 4):
                        if self.stop_event.is_set():
                            f.close()  # Attempt to close file
                            try:
                                # Clean up partial segment
                                os.remove(segment_filename)
                            except OSError:
                                pass
                            return False
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                return True  # Success

            except requests.exceptions.RequestException as e:
                print(
                    f"Error downloading segment {index} URI {segment_uri} (Attempt {attempt + 1}/{attempts}): {e}")
                # Don't retry immediately on 4xx errors
                response_status = getattr(e, 'response', None)
                if response_status is not None and 400 <= response_status.status_code < 500:
                    print(
                        f"Client error {response_status.status_code}, aborting retries for this segment.")
                    return False
                if self.stop_event.is_set():
                    return False
                time.sleep(2 * (attempt + 1))  # Exponential backoff slightly

            except Exception as e:  # Catch other potential errors like file write issues
                print(
                    f"Unexpected error downloading segment {index} (Attempt {attempt + 1}/{attempts}): {e}")
                if self.stop_event.is_set():
                    return False
                time.sleep(2 * (attempt + 1))

        print(f"Failed to download segment {index} after {attempts} attempts.")
        return False

    def _download_subtitle(self, sub_info, session):
        """Downloads a single subtitle track, handling segmented VTT."""
        sub_url = sub_info['uri']
        lang_code = sub_info['lang'] if sub_info['lang'] else 'sub'
        # Use language code primarily, fallback to name if lang is missing/generic 'sub'
        name_part = lang_code if lang_code != 'sub' else sub_info['name']
        # Sanitize the name part used for filename
        safe_name_part = sanitize_filename(
            name_part if name_part else 'subtitle')
        # Construct filename inside the item's output directory
        output_filename_base = os.path.join(
            self.item_output_dir, f"{self.filename_base}.{safe_name_part}")
        output_filename = f"{output_filename_base}.vtt"

        if self.stop_event.is_set():
            return False, None

        final_sub_content = ""
        try:
            self._update_status(f"DL Sub {lang_code}")
            headers = {'User-Agent': USER_AGENT}

            # --- Check if the subtitle URL is an M3U8 playlist ---
            is_sub_m3u8 = urlparse(sub_url).path.lower().endswith('.m3u8')

            if is_sub_m3u8:
                print(
                    f"Subtitle {lang_code} is segmented (m3u8). Fetching segments...")
                # 1. Fetch the subtitle manifest
                sub_manifest_resp = session.get(
                    sub_url, timeout=20, headers=headers)
                sub_manifest_resp.raise_for_status()
                sub_manifest_text = sub_manifest_resp.text
                try:
                    # Use the sub_url itself as the base URI for resolving segment paths
                    sub_manifest = m3u8.loads(sub_manifest_text, uri=sub_url)
                except ValueError:  # Try decoding explicitly if parse fails
                    print(
                        f"Initial parse failed for subtitle manifest {lang_code}, trying UTF-8.")
                    sub_manifest = m3u8.loads(
                        sub_manifest_resp.content.decode('utf-8', 'ignore'), uri=sub_url)

                # 2. Check if it contains segments
                if not sub_manifest.segments:
                    print(
                        f"Warning: Subtitle manifest for {lang_code} contains no segments.")
                    return False, None  # Treat as failure if no segments

                # 3. Download and concatenate segments
                segment_contents = []
                total_sub_segments = len(sub_manifest.segments)
                downloaded_sub_segments = 0
                # Progress for segments
                self._update_status(f"DL Sub {lang_code} Segs", 0)

                for i, segment in enumerate(sub_manifest.segments):
                    if self.stop_event.is_set():
                        raise InterruptedError("Subtitle download stopped")
                    segment_uri = urljoin(sub_manifest.base_uri, segment.uri)
                    try:
                        segment_resp = session.get(
                            segment_uri, timeout=15, headers=headers)
                        segment_resp.raise_for_status()
                        # Assume segments are UTF-8 encoded VTT fragments
                        segment_resp.encoding = segment_resp.apparent_encoding or 'utf-8'
                        segment_text = segment_resp.text
                        segment_contents.append(segment_text)
                        downloaded_sub_segments += 1
                        # Update progress occasionally
                        if downloaded_sub_segments % 5 == 0 or downloaded_sub_segments == total_sub_segments:
                            seg_progress = int(
                                (downloaded_sub_segments / total_sub_segments) * 100)
                            self._update_status(
                                f"DL Sub {lang_code} Segs", seg_progress)

                    except requests.exceptions.RequestException as seg_e:
                        print(
                            f"Warning: Failed subtitle segment {i+1}/{total_sub_segments} for {lang_code}: {seg_e}. Skipping.")

                # 4. Join segments. Ensure WEBVTT header is present only once.
                if not segment_contents:
                    print(
                        f"Warning: No subtitle segments downloaded successfully for {lang_code}.")
                    return False, None

                # Check if the *first* successfully downloaded segment already contains WEBVTT.
                first_segment_has_header = segment_contents[0].strip(
                ).startswith("WEBVTT")

                # Join with double newlines, seems standard for VTT segments
                # Strip leading/trailing whitespace from each segment before joining? Optional.
                full_content_joined = "\n\n".join(
                    s.strip() for s in segment_contents)

                if not first_segment_has_header:
                    final_sub_content = "WEBVTT\n\n" + full_content_joined
                else:
                    # If first segment has header, assume simple join is sufficient
                    final_sub_content = full_content_joined

                print(
                    f"Downloaded and combined {downloaded_sub_segments}/{total_sub_segments} subtitle segments for {lang_code}.")

            else:
                # --- It's likely a direct VTT file URL ---
                print(
                    f"Subtitle {lang_code} appears to be a direct file. Downloading...")
                response = session.get(sub_url, timeout=30, headers=headers)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or 'utf-8'
                final_sub_content = response.text

                # Ensure WEBVTT header
                if not final_sub_content.strip().startswith('WEBVTT'):
                    print(
                        f"Warning: Prepending WEBVTT header to direct subtitle file {lang_code}.")
                    final_sub_content = "WEBVTT\n\n" + final_sub_content

            # --- Save the final content ---
            if not final_sub_content.strip():  # Check if content is empty/whitespace
                print(
                    f"Warning: No actual subtitle content gathered for {lang_code}.")
                return False, None

            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            with open(output_filename, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(final_sub_content)

            print(f"Subtitle saved: {output_filename}")
            return True, output_filename

        except InterruptedError:
            print(f"Subtitle download {lang_code} interrupted.")
            self._update_status(f"Sub {lang_code} Stopped")
            return False, None
        except requests.exceptions.RequestException as e:
            print(
                f"Error during subtitle download {lang_code} ({sub_url}): {e}")
            self._update_status(f"Error Sub {lang_code}")
            return False, None
        except m3u8.ParseError as e:
            print(f"Error parsing subtitle manifest for {lang_code}: {e}")
            self._update_status(f"Error Sub {lang_code} M3U8")
            return False, None
        except Exception as e:
            print(
                f"Error processing/saving subtitle {lang_code} ({sub_url}): {e}")
            traceback.print_exc()  # Print full traceback for unexpected errors
            self._update_status(f"Error Sub {lang_code}")
            try:  # Attempt cleanup
                if os.path.exists(output_filename):
                    os.remove(output_filename)
            except OSError:
                pass
            return False, None

    def _download_and_save_extra_audio(self, audio_info, session):
        """ Uses ffmpeg to directly download and save an audio stream. """
        audio_uri = audio_info['uri']
        # Unknown lang code
        lang_code = audio_info['lang'] if audio_info['lang'] else 'unk'
        # Use lang code or name for filename part
        name_part = lang_code if lang_code != 'unk' else audio_info['name']
        safe_name_part = sanitize_filename(name_part if name_part else 'audio')
        # Output filename like: VideoName.audio.eng.m4a
        output_filename = os.path.join(
            self.item_output_dir, f"{self.filename_base}.audio.{safe_name_part}.m4a")

        if self.stop_event.is_set():
            return False
        self._update_status(f"DL Audio {lang_code}")
        print(
            f"Downloading extra audio track: Lang={lang_code}, Name={name_part}")

        cmd = [
            FFMPEG_PATH,
            '-nostdin',
            # Pass user agent (ensure \r\n for ffmpeg)
            '-headers', f"User-Agent: {USER_AGENT}\r\n",
            '-i', audio_uri,       # Input is the audio playlist URL
            '-c', 'copy',          # Copy codec (no re-encoding)
            '-vn',                 # No video
            '-y',                  # Overwrite output
            output_filename
        ]

        # Ensure output directory exists before calling ffmpeg
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        # Run ffmpeg command, using the helper
        success = self._run_ffmpeg_command(cmd, f"Extra Audio {lang_code}")

        if success:
            if os.path.exists(output_filename) and os.path.getsize(output_filename) > 100:
                print(f"Extra audio track saved: {output_filename}")
                return True
            else:
                print(
                    f"Warning: ffmpeg reported success for audio {lang_code}, but output file is missing/small.")
                self._update_status(f"Error Audio {lang_code}")
                # Optionally remove the bad file
                try:
                    if os.path.exists(output_filename):
                        os.remove(output_filename)
                except OSError:
                    pass
                return False
        else:
            # Error status already set by _run_ffmpeg_command
            return False

    def _download_segments_for_stream(self, media_manifest, stream_type, temp_sub_dir, session):
        """Downloads all segments for a given media manifest (video or primary audio)."""
        if self.stop_event.is_set():
            return False, 0

        segments = media_manifest.segments
        if not segments:
            print(f"Warning: No segments found in {stream_type} playlist.")
            return True, 0  # Not an error, just nothing to download

        total_segments = len(segments)
        print(f"Found {total_segments} {stream_type} segments to download.")
        # Initial status update
        self._update_status(f"DL {stream_type.capitalize()}", 0)

        os.makedirs(temp_sub_dir, exist_ok=True)  # Ensure sub-directory exists

        downloaded_count = 0
        max_workers = 10  # Concurrency level

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_segment = {}
            all_futures = []
            for i, segment in enumerate(segments):
                if self.stop_event.is_set():
                    break
                segment_full_uri = urljoin(
                    media_manifest.base_uri, segment.uri)
                future = executor.submit(
                    self._download_segment, segment_full_uri, temp_sub_dir, i, total_segments, session)
                future_to_segment[future] = i
                all_futures.append(future)

            # Process completed futures
            for future in concurrent.futures.as_completed(all_futures):
                if self.stop_event.is_set():
                    print(
                        f"Stop requested, cancelling pending {stream_type} segment downloads...")
                    for f in all_futures:
                        if not f.done():
                            f.cancel()
                    break  # Exit the completion loop

                segment_index = future_to_segment[future]
                try:
                    if future.cancelled():
                        print(
                            f"{stream_type.capitalize()} segment {segment_index} download was cancelled.")
                        continue

                    success = future.result()
                    if success:
                        downloaded_count += 1
                        progress = int(
                            (downloaded_count / total_segments) * 100)
                        # Update progress less frequently
                        if downloaded_count == total_segments or progress % 5 == 0:
                            self._update_status(
                                f"DL {stream_type.capitalize()}", progress)
                    else:
                        print(
                            f"{stream_type.capitalize()} segment {segment_index} failed permanently.")
                        progress = int(
                            (downloaded_count / total_segments) * 100)
                        self._update_status(
                            f"Error {stream_type[:3]} Seg {segment_index}", progress)

                except concurrent.futures.CancelledError:
                    print(
                        f"{stream_type.capitalize()} segment {segment_index} processing was cancelled.")
                except Exception as exc:
                    print(
                        f'{stream_type.capitalize()} segment {segment_index} generated an exception: {exc}')
                    progress = int((downloaded_count / total_segments) * 100)
                    self._update_status(
                        f"Error {stream_type[:3]} Seg {segment_index}", progress)

        # Final check after loop
        if self.stop_event.is_set():
            print(f"{stream_type.capitalize()} download stopped.")
            final_progress = int(
                (downloaded_count / total_segments) * 100) if total_segments > 0 else 0
            self._update_status("Stopping...", final_progress)
            return False, downloaded_count

        # Check final count
        full_success = (downloaded_count == total_segments)
        if not full_success:
            print(
                f"Warning: Only {downloaded_count}/{total_segments} {stream_type} segments downloaded successfully.")

        final_progress = int((downloaded_count / total_segments)
                             * 100) if total_segments > 0 else 100
        self._update_status(
            f"DL {stream_type.capitalize()} Done", final_progress)

        return full_success, downloaded_count

    def _create_ffmpeg_list_file(self, temp_sub_dir, list_filename, total_segments):
        """Creates the ffmpeg concat list file for segments in a subdirectory."""
        actual_segment_count = 0
        try:
            with open(list_filename, 'w', encoding='utf-8') as f:
                missing_count = 0
                for i in range(total_segments):
                    segment_file = os.path.join(
                        temp_sub_dir, f"segment_{i:05d}.ts")
                    if os.path.exists(segment_file) and os.path.getsize(segment_file) > 0:
                        escaped_path = segment_file.replace(
                            '\\', '/').replace("'", "'\\''")
                        f.write(f"file '{escaped_path}'\n")
                        actual_segment_count += 1
                    else:
                        missing_count += 1
                        # Log first few missing, then just total at the end
                        if missing_count <= 5:
                            print(
                                f"Warning: Segment file {segment_file} missing or empty, skipping merge for it.")

            if actual_segment_count < total_segments:
                print(
                    f"Warning: Found {actual_segment_count}/{total_segments} valid segment files for merging in {temp_sub_dir}. ({missing_count} missing/empty)")
        except Exception as e:
            print(f"Error creating ffmpeg list file {list_filename}: {e}")
            return 0
        return actual_segment_count

    def _run_ffmpeg_command(self, cmd, step_name):
        """Runs an ffmpeg command and checks the result."""
        if self.stop_event.is_set():
            print(f"Skipping ffmpeg {step_name} due to stop request.")
            return False

        print(f"Running ffmpeg command ({step_name}): {' '.join(cmd)}")
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE

        process = None
        try:
            # Capture stderr to check for errors, use utf-8 ignore errors for decoding
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       startupinfo=startupinfo, encoding='utf-8', errors='ignore')
            stdout, stderr = process.communicate()  # Wait for completion

            if process.returncode != 0:
                error_lines = stderr.strip().splitlines()
                brief_error = error_lines[-1] if error_lines else "Unknown ffmpeg error"
                print(
                    f"ffmpeg {step_name} failed! Code: {process.returncode}. Error: {brief_error}")
                # print(f"--- Full ffmpeg stderr ---\n{stderr}\n---") # Uncomment for full debug
                self._update_status(f"Error Merge ({brief_error[:30]})")
                return False
            else:
                # Optional: Check stderr for warnings even on success?
                # if "warning" in stderr.lower():
                #    print(f"ffmpeg {step_name} completed with warnings:\n{stderr[-500:]}") # Log last part of stderr
                print(f"ffmpeg {step_name} successful.")
                return True

        except FileNotFoundError:
            print(f"Error: ffmpeg command ('{FFMPEG_PATH}') not found.")
            self._update_status("Error: ffmpeg not found")
            return False
        except Exception as e:
            print(f"Error running ffmpeg {step_name}: {e}")
            self._update_status(f"Error: ffmpeg execution")
            return False

    def _merge_muxed_ffmpeg(self, video_temp_dir, output_filename, total_segments):
        """Merges segments when audio is assumed to be muxed with video."""
        if self.stop_event.is_set():
            return False
        self._update_status("Merging")

        # Create list file directly in the video segments directory
        list_filename = os.path.join(video_temp_dir, "ffmpeg_list.txt")
        actual_segment_count = self._create_ffmpeg_list_file(
            video_temp_dir, list_filename, total_segments)

        if actual_segment_count == 0:
            print("Error: No valid segments found to merge.")
            self._update_status("Error: No Segments")
            return False

        cmd = [
            FFMPEG_PATH, '-nostdin', '-f', 'concat', '-safe', '0',
            '-i', list_filename,
            # '-fflags', '+genpts', # Sometimes needed if timestamps are bad, potentially slower
            '-c', 'copy', '-y', output_filename
        ]
        success = self._run_ffmpeg_command(cmd, "Muxed Merge")
        if success:
            if not os.path.exists(output_filename) or os.path.getsize(output_filename) < 100:
                print("Warning: ffmpeg success, but output file missing/small.")
                self._update_status("Error Merge (empty file)")
                success = False
        return success

    def _merge_separate_audio_video_ffmpeg(self, temp_dir, output_filename, total_video_segments, total_audio_segments):
        """Merges separate video and primary audio segments."""
        if self.stop_event.is_set():
            return False
        self._update_status("Merging Video/Audio")

        # Define paths to segment directories and list files within the main temp_dir
        video_temp_dir = os.path.join(temp_dir, "video")
        # Specific subdir for primary audio
        audio_temp_dir = os.path.join(temp_dir, "audio_primary")
        video_list_file = os.path.join(temp_dir, "ffmpeg_video_list.txt")
        audio_list_file = os.path.join(
            temp_dir, "ffmpeg_audio_primary_list.txt")

        # Create list files
        actual_video_segments = self._create_ffmpeg_list_file(
            video_temp_dir, video_list_file, total_video_segments)
        actual_audio_segments = self._create_ffmpeg_list_file(
            audio_temp_dir, audio_list_file, total_audio_segments)

        if actual_video_segments == 0:
            print("Error: No valid video segments found to merge.")
            self._update_status("Error: No Video Segments")
            return False

        cmd = []
        step_name = ""
        if actual_audio_segments == 0:
            print("Warning: No valid primary audio segments found. Merging video-only.")
            cmd = [
                FFMPEG_PATH, '-nostdin', '-f', 'concat', '-safe', '0',
                '-i', video_list_file,
                # '-fflags', '+genpts',
                '-c', 'copy', '-y', output_filename
            ]
            step_name = "Video-Only Merge (No Audio Segments)"
        else:
            cmd = [
                FFMPEG_PATH, '-nostdin',
                # Input 0 (Video)
                '-f', 'concat', '-safe', '0', '-i', video_list_file,
                # Input 1 (Audio)
                '-f', 'concat', '-safe', '0', '-i', audio_list_file,
                '-map', '0:v:0?', '-map', '1:a:0?',  # Map first video and audio streams
                # '-fflags', '+genpts', # Add if needed for timestamp issues
                '-c', 'copy', '-y', output_filename
            ]
            step_name = "Separate Audio/Video Merge"

        success = self._run_ffmpeg_command(cmd, step_name)
        if success:
            if not os.path.exists(output_filename) or os.path.getsize(output_filename) < 100:
                print("Warning: ffmpeg success, but output file missing/small.")
                self._update_status("Error Merge (empty file)")
                success = False
        return success

    def run_download(self):
        """Main download execution function - Handles folders, all subs, all audio."""
        if not is_ffmpeg_installed():
            self._update_status("Error: ffmpeg not found!")
            print(f"FATAL: ffmpeg command ('{FFMPEG_PATH}') not found.")
            return

        if self.stop_event.is_set():
            self._update_status("Stopped")
            return

        # --- Initialization ---
        try:
            os.makedirs(self.item_output_dir, exist_ok=True)
        except OSError as e:
            print(
                f"FATAL: Cannot create item output directory {self.item_output_dir}: {e}")
            self._update_status(f"Error: Create Dir")
            return

        temp_dir = os.path.join(self.item_output_dir,
                                f".tmp_{int(time.time())}")
        final_output_video = os.path.join(
            self.item_output_dir, f"{self.filename_base}.mp4")
        downloaded_sub_paths = []
        primary_audio_info = None
        all_audio_infos = []
        video_playlist_obj = None
        video_media_manifest = None
        primary_audio_media_manifest = None
        total_video_segments = 0
        total_primary_audio_segments = 0
        extra_audio_success_count = 0  # Count successful extra audio downloads
        session = requests.Session()

        try:
            os.makedirs(temp_dir, exist_ok=True)

            self._update_status("Fetching Manifest")
            headers = {'User-Agent': USER_AGENT}
            manifest_response = session.get(
                self.m3u8_url, timeout=(10, 20), headers=headers)
            manifest_response.raise_for_status()
            manifest_content = manifest_response.text
            try:
                master_manifest = m3u8.loads(
                    manifest_content, uri=self.m3u8_url)
            except ValueError:
                manifest_content = manifest_response.content.decode(
                    'utf-8', errors='ignore')
                master_manifest = m3u8.loads(
                    manifest_content, uri=self.m3u8_url)

            # --- Select Streams and Get Info ---
            video_playlist_obj, primary_audio_info, all_audio_infos = get_video_and_audio_playlists(
                master_manifest, self.preferred_res)
            all_subtitle_infos = get_all_subtitle_playlists(
                master_manifest) if self.download_subs else []

            # --- Get Video Manifest ---
            if not video_playlist_obj:
                if master_manifest.segments:
                    video_media_manifest = master_manifest
                    primary_audio_info = None
                    all_audio_infos = []
                else:
                    raise ValueError("Could not determine video stream.")
            elif isinstance(video_playlist_obj, m3u8.model.Playlist):
                video_playlist_uri = urljoin(
                    master_manifest.base_uri, video_playlist_obj.uri)
                self._update_status("Fetching Video Playlist")
                media_playlist_response = session.get(
                    video_playlist_uri, timeout=15, headers=headers)
                media_playlist_response.raise_for_status()
                video_media_manifest = m3u8.loads(
                    media_playlist_response.text, uri=video_playlist_uri)
            elif isinstance(video_playlist_obj, m3u8.model.M3U8):
                video_media_manifest = video_playlist_obj

            if not video_media_manifest or not video_media_manifest.segments:
                raise ValueError("Selected video stream contains no segments.")
            total_video_segments = len(video_media_manifest.segments)

            # --- Get Primary Audio Manifest ---
            if primary_audio_info:
                self._update_status("Fetching Primary Audio Playlist")
                try:
                    primary_audio_uri = primary_audio_info['uri']
                    audio_playlist_response = session.get(
                        primary_audio_uri, timeout=15, headers=headers)
                    audio_playlist_response.raise_for_status()
                    primary_audio_media_manifest = m3u8.loads(
                        audio_playlist_response.text, uri=primary_audio_uri)
                    if not primary_audio_media_manifest.segments:
                        print("Warning: Primary audio playlist empty.")
                        primary_audio_info = None
                        total_primary_audio_segments = 0
                    else:
                        total_primary_audio_segments = len(
                            primary_audio_media_manifest.segments)
                except (requests.exceptions.RequestException, m3u8.ParseError) as e:
                    print(f"Warning: Failed fetch/parse primary audio ({e}).")
                    primary_audio_info = None
                    total_primary_audio_segments = 0

            # --- Download Subtitles (concurrently) ---
            if all_subtitle_infos:
                print(
                    f"Starting download of {len(all_subtitle_infos)} subtitle track(s)...")
                with concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="SubDL") as sub_executor:
                    future_to_sub = {sub_executor.submit(
                        self._download_subtitle, sub_info, session): sub_info for sub_info in all_subtitle_infos}
                    for future in concurrent.futures.as_completed(future_to_sub):
                        if self.stop_event.is_set():
                            future.cancel()
                            continue  # Check stop event during sub downloads
                        sub_info = future_to_sub[future]
                        try:
                            success, saved_path = future.result()
                            if success and saved_path:
                                downloaded_sub_paths.append(saved_path)
                        except concurrent.futures.CancelledError:
                            pass  # Ignore cancelled futures
                        except Exception as exc:
                            print(
                                f"Subtitle DL ({sub_info.get('lang')}) error: {exc}")

            if self.stop_event.is_set():
                raise InterruptedError("Stopped during subtitle download")

            # --- Download Video Segments ---
            if self.stop_event.is_set():
                raise InterruptedError("Stopped before video download")
            video_temp_dir = os.path.join(
                temp_dir, "video") if primary_audio_info else temp_dir
            video_dl_full_success, downloaded_video_count = self._download_segments_for_stream(
                video_media_manifest, "video", video_temp_dir, session
            )
            if downloaded_video_count == 0 and not self.stop_event.is_set():
                raise ValueError("Video download failed completely.")

            if self.stop_event.is_set():
                raise InterruptedError("Stopped during video download")

            # --- Download Primary Audio Segments ---
            primary_audio_dl_full_success = True
            downloaded_primary_audio_count = 0
            primary_audio_temp_dir = None
            if primary_audio_info and primary_audio_media_manifest:
                primary_audio_temp_dir = os.path.join(
                    temp_dir, "audio_primary")
                primary_audio_dl_full_success, downloaded_primary_audio_count = self._download_segments_for_stream(
                    primary_audio_media_manifest, "primary audio", primary_audio_temp_dir, session
                )

            if self.stop_event.is_set():
                raise InterruptedError("Stopped during primary audio download")

            # --- Download Extra Audio Tracks (concurrently) ---
            extra_audio_infos = [
                a for a in all_audio_infos if a != primary_audio_info]
            if extra_audio_infos:
                print(
                    f"Starting download of {len(extra_audio_infos)} extra audio track(s)...")
                with concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="AudioDL") as extra_audio_executor:
                    future_to_audio = {extra_audio_executor.submit(
                        self._download_and_save_extra_audio, audio_info, session): audio_info for audio_info in extra_audio_infos}
                    for future in concurrent.futures.as_completed(future_to_audio):
                        if self.stop_event.is_set():
                            future.cancel()
                            continue  # Check stop event
                        audio_info = future_to_audio[future]
                        try:
                            success = future.result()
                            if success:
                                extra_audio_success_count += 1
                        except concurrent.futures.CancelledError:
                            pass  # Ignore cancelled futures
                        except Exception as exc:
                            print(
                                f"Extra audio DL ({audio_info.get('lang')}) error: {exc}")

            if self.stop_event.is_set():
                raise InterruptedError("Stopped during extra audio download")

            # --- Merge Video + Primary Audio ---
            if self.stop_event.is_set():
                raise InterruptedError("Stop requested before merge")
            merge_successful = False
            if downloaded_video_count == 0:
                print("No video segments downloaded. Cannot merge MP4.")
                self._update_status("Error: No Video Segments DL")
            elif primary_audio_info and downloaded_primary_audio_count > 0:
                merge_successful = self._merge_separate_audio_video_ffmpeg(
                    temp_dir, final_output_video, total_video_segments, total_primary_audio_segments
                )
            else:
                print("Merging video (primary audio muxed, missing, or failed)...")
                merge_successful = self._merge_muxed_ffmpeg(
                    video_temp_dir, final_output_video, total_video_segments
                )

            # --- Final Status ---
            if merge_successful:
                self._update_status("Completed", 100)
                # Show relative path
                final_message = f"Success: {self.filename_base}{os.sep}{self.filename_base}.mp4"
                if downloaded_sub_paths:
                    final_message += f" | Subs: {len(downloaded_sub_paths)}"
                # Count total audio tracks successfully saved (primary in MP4 + extras)
                num_primary_audio = 1 if (primary_audio_info and downloaded_primary_audio_count > 0) or (
                    # Count primary if merged or muxed (and video exists)
                    not primary_audio_info and downloaded_video_count > 0) else 0
                total_successful_audio = num_primary_audio + extra_audio_success_count
                if total_successful_audio > 0:
                    final_message += f" | Audios: {total_successful_audio}"
                print(final_message)

        # --- Exception Handling ---
        except InterruptedError as e:
            print(f"Operation interrupted: {e}")  # Status set elsewhere
        except requests.exceptions.RequestException as e:
            print(f"Network Error: {e}")
            self._update_status(f"Error: Network")
        except m3u8.ParseError as e:
            print(f"M3U8 Parsing Error: {e}")
            self._update_status(f"Error: Invalid M3U8")
        except ValueError as e:
            print(f"Value Error: {e}")
            self._update_status(f"Error: {e}")
        except Exception as e:
            print(f"Unexpected error in run_download: {e}")
            traceback.print_exc()
            self._update_status(f"Error: Unexpected")

        # --- Cleanup ---
        finally:
            session.close()
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                except OSError as e:
                    print(f"Warning: Error cleaning temp dir {temp_dir}: {e}")

            # --- Crucial: Signal that this download thread has finished ---
            if self.item_id:
                self.gui_queue.put({"id": self.item_id, "status": "FINISHED"})
            # --- End Signal ---


# --- GUI Application Class ---
class DownloadManagerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Simple m3u8 Downloader")
        self.root.geometry("850x650")

        self.output_directory = tk.StringVar(
            value=os.path.join(os.getcwd(), "Downloads"))
        self.preferred_resolution = tk.StringVar(value=DEFAULT_RESOLUTION)
        self.download_subtitles = tk.BooleanVar(value=True)
        self.download_items = {}
        self.download_threads = {}
        self.downloader_instances = {}
        self.gui_queue = queue.Queue()

        # --- Queue State ---
        self.active_download_count = 0
        self.max_concurrent_var = tk.IntVar(
            value=4)  # Default concurrency limit
        self.queue_processing_enabled = False  # Queue is initially paused
        # --- End Queue State ---

        self.style = ttk.Style(root)
        try:
            available_themes = self.style.theme_names()
            if 'vista' in available_themes:
                self.style.theme_use('vista')
            elif 'clam' in available_themes:
                self.style.theme_use('clam')
            elif 'aqua' in available_themes:
                self.style.theme_use('aqua')
            elif 'gtk+' in available_themes:
                self.style.theme_use('gtk+')
        except Exception as e:
            print(f"Could not set theme: {e}")

        self._create_widgets()
        self._check_ffmpeg()
        # Add app stop event for graceful shutdown checks
        self.stop_event = threading.Event()
        self.root.after(100, self.process_gui_queue)
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _check_ffmpeg(self):
        if not is_ffmpeg_installed():
            messagebox.showwarning("ffmpeg Not Found",
                                   f"ffmpeg command ('{FFMPEG_PATH}') not found or not executable.\n\n"
                                   "Video/Audio merging and extra audio download will fail.\n\n"
                                   "Please install ffmpeg and ensure it is in your system's PATH.",
                                   parent=self.root)

    def _create_widgets(self):
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.pack(fill=tk.X, side=tk.TOP)
        ttk.Label(top_frame, text="Import Links (JSON list or URL per line):").pack(
            anchor=tk.W)
        self.link_input = scrolledtext.ScrolledText(
            top_frame, height=6, width=80, relief=tk.SOLID, borderwidth=1)
        self.link_input.pack(fill=tk.X, expand=True, padx=1, pady=1)
        import_button = ttk.Button(
            top_frame, text="Import Links to Queue", command=self.import_links)
        import_button.pack(side=tk.LEFT, pady=(5, 0))

        settings_frame = ttk.Frame(self.root, padding="5 10")
        settings_frame.pack(fill=tk.X, side=tk.TOP)
        ttk.Label(settings_frame, text="Output Dir:").grid(
            row=0, column=0, padx=2, pady=5, sticky=tk.W)
        output_entry = ttk.Entry(
            settings_frame, textvariable=self.output_directory, width=50)
        output_entry.grid(row=0, column=1, padx=2, pady=5, sticky=tk.EW)
        browse_button = ttk.Button(
            settings_frame, text="Browse...", command=self.browse_output_dir)
        browse_button.grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(settings_frame, text="Pref. Res:").grid(
            row=1, column=0, padx=2, pady=5, sticky=tk.W)
        res_options = ["Best", "1080", "720", "540", "480", "360"]
        res_combobox = ttk.Combobox(
            settings_frame, textvariable=self.preferred_resolution, values=res_options, width=7, state="readonly")
        res_combobox.grid(row=1, column=1, padx=2, pady=5, sticky=tk.W)
        self.preferred_resolution.set(DEFAULT_RESOLUTION)
        sub_check = ttk.Checkbutton(
            settings_frame, text="Download All Subtitles", variable=self.download_subtitles)
        sub_check.grid(row=1, column=1, padx=(90, 0),
                       pady=5, sticky=tk.W, columnspan=2)

        # Concurrency Limit Setting
        ttk.Label(settings_frame, text="Concurrent DLs:").grid(
            row=2, column=0, padx=2, pady=5, sticky=tk.W)
        concurrency_spinbox = ttk.Spinbox(settings_frame, from_=1, to=20, textvariable=self.max_concurrent_var,
                                          width=5, state="readonly", command=self._on_concurrency_change)
        concurrency_spinbox.grid(row=2, column=1, padx=2, pady=5, sticky=tk.W)

        settings_frame.columnconfigure(1, weight=1)

        action_frame = ttk.Frame(self.root, padding="5 10")
        action_frame.pack(fill=tk.X, side=tk.TOP)
        self.start_pause_queue_button = ttk.Button(action_frame, text="Start Queue", command=self.toggle_queue_processing)
        self.start_pause_queue_button.pack(side=tk.LEFT, padx=5)
        start_button = ttk.Button(action_frame, text="Start Selected", command=self.start_selected_downloads)
        start_button.pack(side=tk.LEFT, padx=5)
        stop_button = ttk.Button(action_frame, text="Stop Selected", command=self.stop_selected_downloads)
        stop_button.pack(side=tk.LEFT, padx=5)
        spacer = ttk.Frame(action_frame); spacer.pack(side=tk.LEFT, expand=True, fill=tk.X)
        clear_completed_button = ttk.Button(action_frame, text="Clear Completed", command=self.clear_completed_items); clear_completed_button.pack(side=tk.RIGHT, padx=5)
        remove_button = ttk.Button(action_frame, text="Remove Selected", command=self.remove_selected_items); remove_button.pack(side=tk.RIGHT, padx=5)

        list_frame = ttk.Frame(self.root, padding="10 0 10 10")
        list_frame.pack(fill=tk.BOTH, expand=True, side=tk.TOP)
        self.tree = ttk.Treeview(list_frame, columns=(
            "Name", "URL", "Status", "Progress"), show="headings")
        self.tree.heading("Name", text="Name",
                          command=lambda: self.sort_column("Name", False))
        self.tree.heading("URL", text="URL")
        self.tree.heading("Status", text="Status",
                          command=lambda: self.sort_column("Status", False))
        self.tree.heading("Progress", text="Progress",
                          command=lambda: self.sort_column("Progress", False))
        self.tree.column("Name", width=220, anchor=tk.W, stretch=tk.NO)
        self.tree.column("URL", width=300, anchor=tk.W, stretch=tk.YES)
        self.tree.column("Status", width=150, anchor=tk.W, stretch=tk.NO)
        self.tree.column("Progress", width=80, anchor=tk.CENTER, stretch=tk.NO)
        vsb = ttk.Scrollbar(list_frame, orient="vertical",
                            command=self.tree.yview)
        hsb = ttk.Scrollbar(list_frame, orient="horizontal",
                            command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        list_frame.grid_rowconfigure(0, weight=1)
        list_frame.grid_columnconfigure(0, weight=1)

        # Initial Status Bar Message
        self.status_var = tk.StringVar(
            value=f"Ready (Queue Paused | Max {self.max_concurrent_var.get()} concurrent)")
        status_bar = ttk.Label(self.root, textvariable=self.status_var,
                               relief=tk.SUNKEN, anchor=tk.W, padding="2 5")
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # --- GUI Methods ---
    def _update_active_status(self):
        """Updates the status bar to show current active/limit and queue state."""
        try:
            limit = self.max_concurrent_var.get()
            queue_state = "Running" if self.queue_processing_enabled else "Paused"
            # Determine if truly idle (no active, no pending)
            has_pending = any(
                info["status"] == "Pending" for info in self.download_items.values())
            if not self.queue_processing_enabled and self.active_download_count == 0 and not has_pending:
                queue_state = "Idle"
            self.status_var.set(
                f"Queue: {queue_state} | Active: {self.active_download_count}/{limit}")
        except (tk.TclError, AttributeError):
            self.status_var.set("Status unavailable...")

    def _on_concurrency_change(self):
        """Called when the concurrency spinbox value changes."""
        self._update_active_status()
        # If queue is running and limit increased, check if we can start more
        if self.queue_processing_enabled:
            self._check_and_start_pending()

    def toggle_queue_processing(self):
        """Starts or pauses the automatic processing of the download queue."""
        self.queue_processing_enabled = not self.queue_processing_enabled
        if self.queue_processing_enabled:
            print("Queue processing ENABLED.")
            self.start_pause_queue_button.config(text="Pause Queue")
            self._check_and_start_pending()  # Immediately check if we can start downloads
        else:
            print("Queue processing PAUSED.")
            self.start_pause_queue_button.config(text="Start Queue")
        self._update_active_status()  # Update status bar

    def sort_column(self, col, reverse):
        try:
            l = []  # Store tuples of (value, item_id)
            for k in self.tree.get_children(''):
                try:
                    l.append((self.tree.set(k, col), k))
                except tk.TclError:
                    continue  # Skip deleted item
            if col == "Progress":
                def get_progress_val(t):
                    try:
                        return int(t[0].replace('%', '').strip())
                    except:
                        return -1  # Sort errors first
                l.sort(key=get_progress_val, reverse=reverse)
            else:
                l.sort(key=lambda t: str(t[0]).lower(), reverse=reverse)
            for index, (val, k) in enumerate(l):
                try:
                    self.tree.move(k, '', index)
                except tk.TclError:
                    continue  # Skip deleted item
            self.tree.heading(
                col, command=lambda: self.sort_column(col, not reverse))
        except Exception as e:
            print(f"Error sorting column {col}: {e}")

    def browse_output_dir(self):
        directory = filedialog.askdirectory(
            initialdir=self.output_directory.get(), parent=self.root)
        if directory:
            self.output_directory.set(directory)
            self._update_active_status()

    def import_links(self):
        input_text = self.link_input.get("1.0", tk.END).strip()
        if not input_text:
            self.status_var.set("Input area empty.")
            return

        added_count = 0
        links_to_add = []  # Collect items to add before processing

        try:
            # Attempt to parse input as JSON
            data = json.loads(input_text)

            # --- Process parsed JSON data ---
            if isinstance(data, list):
                # Input is a JSON list
                for item in data:
                    if isinstance(item, dict) and "url" in item:
                        # Item is a dictionary like {"name": "...", "url": "..."}
                        url = item.get("url", "").strip()
                        if url:
                            links_to_add.append(
                                {"name": item.get("name"), "url": url})
                    elif isinstance(item, str) and item.strip().lower().startswith(('http', 'ftp')) and '.m3u8' in item.strip().lower():
                        # Item is a string that looks like a valid URL
                        url = item.strip()
                        # Name will be generated later
                        links_to_add.append({"name": None, "url": url})
                    else:
                        # Skip items in the list that are neither dicts with URL nor valid URL strings
                        print(f"Skipping invalid JSON list item: {item}")

            elif isinstance(data, dict) and "url" in data:
                # Input is a single JSON object like {"name": "...", "url": "..."}
                url = data.get("url", "").strip()
                if url:
                    links_to_add.append({"name": data.get("name"), "url": url})
            else:
                # JSON was valid but not in expected format (list or single object with url)
                raise ValueError(
                    "JSON input must be a list (of URLs or {name:, url:} objects) or a single {name:, url:} object.")

        except json.JSONDecodeError:
            # --- Input is NOT valid JSON, treat as newline-separated URLs ---
            print("Input not valid JSON, treating as newline-separated URLs.")
            lines = input_text.splitlines()
            for line in lines:
                url = line.strip()
                # Basic validation for URL format and m3u8 extension
                if url and url.lower().startswith(('http', 'ftp')) and '.m3u8' in url.lower():
                    # Name will be generated later
                    links_to_add.append({"name": None, "url": url})
                elif url:
                    # Log skipped lines that aren't empty but don't look right
                    print(f"Skipping invalid/non-m3u8 URL line: {url}")

        except AttributeError as e:
            # Catch the specific error if somehow missed by type checks
            messagebox.showerror(
                "Import Error", f"Import failed: Invalid data structure in JSON list?\n({e})", parent=self.root)
            self._update_active_status()  # Update status bar
            return
        except Exception as e:
            # Catch other potential errors during processing
            messagebox.showerror(
                "Import Error", f"Import failed: {e}", parent=self.root)
            self._update_active_status()  # Update status bar
            return

        # --- Generate names if missing and add collected items to download list ---
        # Better default naming counter, considering existing DL_ items
        default_name_counter = sum(
            1 for item_id in self.download_items if self.download_items[item_id]['name'].startswith("DL_")) + 1
        for link_info in links_to_add:
            url = link_info["url"]
            name = link_info["name"]  # Can be None initially

            # Generate name only if it's missing (was None)
            if name is None:
                try:
                    # Attempt to create a descriptive name from URL components
                    path = urlparse(url).path
                    base = os.path.basename(path)
                    gen_name = os.path.splitext(base)[0] if base else ""
                    # Fallback if generated name is generic or empty
                    if not gen_name or gen_name.lower() in ('video', 'manifest', 'playlist', 'index', 'master', 'chunklist'):
                        parent = os.path.basename(os.path.dirname(path))
                        gen_name = parent if parent else f"DL_{default_name_counter}"
                    name = gen_name.replace('-', ' ').replace('_', ' ').strip()
                    default_name_counter += 1
                except Exception:
                    # Absolute fallback if URL parsing fails
                    name = f"DL_{default_name_counter}"
                    default_name_counter += 1

            # Add the item (with original or generated name) to the download list
            if self.add_download_item(name, url):
                added_count += 1

        # --- Final status update ---
        if added_count > 0:
            # Clear input area only if successful adds
            self.link_input.delete("1.0", tk.END)
            # Update main status
            self.status_var.set(f"Added {added_count} items.")
        else:
            self.status_var.set("No valid new links found or added.")
        self._update_active_status()  # Update active download count status

    def add_download_item(self, name, url):
        item_id = url
        if item_id in self.download_items:
            self._update_active_status()
            return False
        safe_name = sanitize_filename(name)
        try:
            tree_id = self.tree.insert(
                "", tk.END, iid=item_id, values=(safe_name, url, "Pending", "0%"))
            self.download_items[item_id] = {
                "tree_id": tree_id, "name": safe_name, "url": url, "status": "Pending", "progress": 0}
            return True
        except Exception as e:
            print(f"Error adding {name} to treeview: {e}")
            return False

    def get_selected_item_ids(self): return self.tree.selection()

    def start_single_download(self, item_id):
        """Starts a single download if prerequisites met. Increments active count."""
        output_dir = self.output_directory.get()
        if not output_dir:
            messagebox.showerror(
                "Error", "Base Output directory not set.", parent=self.root)
            self.update_item_status(item_id, "Error: No Output Dir", 0)
            return False
        try:
            os.makedirs(output_dir, exist_ok=True)
        except OSError as e:
            messagebox.showerror(
                "Error", f"Cannot create base output directory: {e}", parent=self.root)
            self.update_item_status(item_id, "Error: Output Dir Fail", 0)
            return False

        if item_id in self.download_items:
            task_info = self.download_items[item_id]
            name = task_info["name"]
            url = task_info["url"]
            self.update_item_status(item_id, "Starting...", 0)
            pref_res = self.preferred_resolution.get()
            pref_res = None if pref_res.lower() == "best" else pref_res
            download_subs = self.download_subtitles.get()

            downloader = Downloader(url=url, output_dir=output_dir, filename_base=name,
                                    preferred_res=pref_res, download_subs=download_subs, gui_queue=self.gui_queue)
            downloader.item_id = item_id
            self.downloader_instances[item_id] = downloader
            thread = threading.Thread(
                target=downloader.run_download, daemon=True, name=f"DL-{name[:10]}")
            self.download_threads[item_id] = thread

            self.active_download_count += 1
            thread.start()  # Increment BEFORE start
            print(
                f"Started download for {name}. Active: {self.active_download_count}/{self.max_concurrent_var.get()}")
            self._update_active_status()
            return True
        else:
            print(f"Cannot start download: Item {item_id} not found.")
            return False

    def _check_and_start_pending(self):
        """Checks if new downloads can start (if queue enabled) and starts next pending."""
        if self.stop_event.is_set():
            return  # App closing
        if not self.queue_processing_enabled:
            self._update_active_status()
            return  # Queue paused
        try:
            limit = self.max_concurrent_var.get()
        except (tk.TclError, AttributeError):
            limit = 0

        while self.active_download_count < limit:
            next_pending_id = None
            # Find next PENDING in visual order
            for item_id in self.tree.get_children(''):
                if item_id in self.download_items and self.download_items[item_id]["status"] == "Pending":
                    next_pending_id = item_id
                    break
            if next_pending_id:
                print(
                    f"Queue: Slot available. Starting: {self.download_items[next_pending_id]['name']}")
                if not self.start_single_download(next_pending_id):
                    break  # Stop if start fails
            else:
                break  # No more pending items
        # Update status bar after checking/starting
        self._update_active_status()

    def start_selected_downloads(self):
        """Starts selected PENDING downloads up to the concurrency limit. Ignores queue enabled state."""
        selected_ids = self.get_selected_item_ids()
        if not selected_ids:
            self._update_active_status()
            return
        started_count = 0
        limit = self.max_concurrent_var.get()
        processed_count = 0
        limit_hit = False

        for item_id in selected_ids:
            processed_count += 1
            if item_id in self.download_items and self.download_items[item_id]["status"] == "Pending":
                if self.active_download_count < limit:
                    if self.start_single_download(item_id):
                        started_count += 1
                else:
                    limit_hit = True
                    break  # Stop trying if limit reached

        status_msg = ""
        if started_count > 0:
            status_msg += f"Started {started_count} selected. "
        if limit_hit:
            status_msg += f"Limit ({limit}) reached. "
        if not status_msg and selected_ids:
            status_msg = "Selected items not pending or limit reached. "

        if status_msg:
            self.status_var.set(
                status_msg + f"Active: {self.active_download_count}/{limit}")
        else:
            self._update_active_status()  # Fallback update

    def stop_downloads(self, item_ids_to_stop):
        """Signals downloader threads to stop."""
        stopped_count = 0
        for item_id in item_ids_to_stop:
            task_info = self.download_items.get(item_id)
            if not task_info:
                continue
            if item_id in self.downloader_instances:
                downloader = self.downloader_instances[item_id]
                if item_id in self.download_threads and self.download_threads[item_id].is_alive():
                    if not downloader.stop_event.is_set():
                        downloader.stop_event.set()
                        self.update_item_status(item_id, "Stopping...")
                        stopped_count += 1
                elif task_info["status"] not in ["Completed", "Stopped", "Error", "Pending"]:
                    self.update_item_status(item_id, "Stopped")
                    stopped_count += 1
            elif task_info["status"] in ["Pending", "Queued"]:
                self.update_item_status(item_id, "Stopped")
                stopped_count += 1
        if stopped_count > 0:
            self._update_active_status()
        elif item_ids_to_stop:
            self.status_var.set("Selected items not stoppable.")

    def stop_selected_downloads(self):
        selected_ids = self.get_selected_item_ids()
        if not selected_ids:
            self._update_active_status()
            return
        self.stop_downloads(selected_ids)

    def remove_selected_items(self):
        selected_ids = self.get_selected_item_ids()
        if not selected_ids:
            self._update_active_status()
            return
        active_ids = [id for id in selected_ids if self.download_items.get(
            id, {}).get("status") not in ["Pending", "Completed", "Stopped", "Error"]]
        confirm = True
        if active_ids:
            confirm = messagebox.askyesno(
                "Confirm Removal", f"{len(active_ids)} item(s) might be active. Stop before removing?", parent=self.root)
        if confirm:
            if active_ids:
                self.stop_downloads(active_ids)
                time.sleep(0.1)
            removed_count = 0
            ids_to_delete = list(selected_ids)
            needs_queue_check = False
            for item_id in ids_to_delete:
                if item_id in self.download_items:
                    task_info = self.download_items[item_id]
                    was_counted_active = task_info["status"] not in [
                        "Pending", "Completed", "Stopped", "Error"]
                    if item_id in self.downloader_instances:
                        self.downloader_instances[item_id].stop_event.set()
                    if was_counted_active:  # Manually adjust count ONLY if removing an item that was active
                        thread_alive = item_id in self.download_threads and self.download_threads[item_id].is_alive(
                        )
                        if not thread_alive and self.active_download_count > 0:  # If wasn't running, won't send FINISHED
                            self.active_download_count -= 1
                            needs_queue_check = True
                            print(
                                f"Manually decremented active count for removed item {item_id}. Active: {self.active_download_count}")

                    tree_id = task_info["tree_id"]
                    if self.tree.exists(tree_id):
                        self.tree.delete(tree_id)
                    if item_id in self.download_threads:
                        del self.download_threads[item_id]
                    if item_id in self.downloader_instances:
                        del self.downloader_instances[item_id]
                    del self.download_items[item_id]
                    removed_count += 1
            if removed_count > 0:
                self._update_active_status()  # Update status bar
            if needs_queue_check:
                self._check_and_start_pending()  # Check queue if slots might be free
        else:
            self.status_var.set("Removal cancelled.")

    def clear_completed_items(self):
        completed_ids = [id for id, info in self.download_items.items(
        ) if info["status"] == "Completed"]
        if not completed_ids:
            self._update_active_status()
            return
        removed_count = 0
        for item_id in completed_ids:
            if item_id in self.download_items:
                tree_id = self.download_items[item_id]["tree_id"]
                if self.tree.exists(tree_id):
                    self.tree.delete(tree_id)
                if item_id in self.download_threads:
                    del self.download_threads[item_id]
                if item_id in self.downloader_instances:
                    del self.downloader_instances[item_id]
                del self.download_items[item_id]
                removed_count += 1
        if removed_count > 0:
            self._update_active_status()

    def update_item_status(self, item_id, status, progress=None):
        if item_id in self.download_items:
            task_info = self.download_items[item_id]
            tree_id = task_info["tree_id"]
            if not self.tree.exists(tree_id):
                return  # Item removed from UI
            try:
                current_values = list(self.tree.item(tree_id, 'values'))
                needs_update = False
                if current_values[2] != status:
                    current_values[2] = status
                    task_info["status"] = status
                    needs_update = True
                if progress is not None:
                    safe_progress = max(0, min(100, int(progress)))
                    new_progress_str = f"{safe_progress}%"
                    if current_values[3] != new_progress_str:
                        current_values[3] = new_progress_str
                        task_info["progress"] = safe_progress
                        needs_update = True
                if needs_update:
                    self.tree.item(tree_id, values=tuple(current_values))
            except tk.TclError as e:
                print(f"TclError updating item {item_id}: {e}.")
            except Exception as e:
                print(f"Error updating GUI for {item_id}: {e}")

    def process_gui_queue(self):
        """Processes status messages AND finish signals from download threads."""
        try:
            while True:  # Process all available messages
                update = self.gui_queue.get_nowait()
                item_id = update.get("id")
                status = update.get("status")
                progress = update.get("progress")
                if not item_id:
                    continue

                # --- Handle the special FINISHED signal ---
                if status == "FINISHED":
                    # Check if we were tracking this instance when it finished
                    if item_id in self.downloader_instances:
                        if self.active_download_count > 0:
                            self.active_download_count -= 1
                            print(
                                f"Download finished/stopped ({self.download_items.get(item_id, {}).get('name', '?')}). Active: {self.active_download_count}/{self.max_concurrent_var.get()}")
                            # Since a slot is free, check queue
                            self._check_and_start_pending()
                        else:
                            print(
                                "Warning: FINISHED signal received but active_count was already 0.")
                        # Remove instance ref now? Maybe better than leaving dangling refs?
                        # if item_id in self.downloader_instances: del self.downloader_instances[item_id]
                    # else: Instance might have been removed already. Count adjusted manually?

                # --- Handle regular status updates ---
                else:
                    self.update_item_status(item_id, status, progress)

        except queue.Empty:
            pass  # No messages left
        except Exception as e:
            print(f"Error processing GUI queue: {e}")
            traceback.print_exc()
        finally:
            self.root.after(100, self.process_gui_queue)  # Reschedule

    def on_closing(self):
        """Handle window close event (WM_DELETE_WINDOW)."""
        self.stop_event.set()  # Signal background checks to stop
        if self.active_download_count > 0:
            if messagebox.askokcancel("Quit", f"{self.active_download_count} downloads active. Quit anyway?", parent=self.root):
                print("Stopping active downloads on exit...")
                for instance in self.downloader_instances.values():
                    instance.stop_event.set()
                # Wait a very brief moment allows threads to potentially acknowledge stop
                # time.sleep(0.1) # Optional small delay
                self.root.destroy()
        else:
            self.root.destroy()


# --- Main Execution ---
if __name__ == "__main__":
    def show_error(exc_type, exc_value, tb):  # Basic Tkinter exception handler
        message = f"Unhandled Exception:\n{exc_type.__name__}: {exc_value}\n"
        message += "".join(traceback.format_tb(tb))
        print(message)  # Print to console
        try:
            # Show in GUI if possible
            messagebox.showerror("Unhandled Exception", message[:2000])
        except:
            pass  # Avoid errors during error reporting
    tk.Tk.report_callback_exception = show_error

    root = tk.Tk()
    app = DownloadManagerApp(root)
    root.mainloop()
