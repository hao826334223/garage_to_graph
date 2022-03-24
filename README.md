# Load Sql into Graph

### Run (standalone)
- update `<ip>` in `load/config.yaml` with your PC ip. You can get it by running `hostname -I`
- run `make` which does the following:
  - build and run mysql image with `garage-order-20220103.sql` loaded
  - build and run gremlin image
  - build and run load image which gets data from mysql, organize it, and load it into gremlin  

### Run (after Digital Twin) 
i.e. running without gremlin because digital twin will run it

- update `<ip>` in `load/config.yaml` with your PC ip. You can get it by running `hostname -I`
- run `make mysql load` which does the following:
  - build and run mysql image with `garage-order-20220103.sql` loaded
  - build and run load image which gets data from mysql, organize it, and load it into gremlin  
