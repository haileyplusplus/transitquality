# Chicago Transit Arrival Estimates

This is an in-progress project to provide estimates of transit vehicle arrivals in Chicago. This 
project includes features not commonly seen in other trackers:

- Consideration of walking time to stops when showing nearby vehicles. The app uses an on-street 
  routing engine (Valhalla) to calculate pedestrian walking time from the input location to the 
  transit stop, both displaying this estimated time and filtering out vehicles that will have 
  arrived at the stop sooner than the estimated walking time.
- Display of vehicle arrival estimates as a time range rather than a single number. The backend 
  keeps a history of recent vehicle arrivals and uses the distribution of recent journey times 
  from a given position on the route to the desired stop to calculate this estimate range. This 
  can lead to a better user experience, especially when taking buses which tend to have a more 
  variable arrival time.

Initial feature work is largely complete. Current efforts are focused on making the backend 
stable and performant enough for generally reliable use, as well as adding observability and 
testing.

## Screenshots

### Route selection overview

![Route selection overview](screenshots/route_overview_screenshot.png)


### Arrival prediction detail

![Arrival prediction detail](screenshots/route_detail_screenshot.png)
