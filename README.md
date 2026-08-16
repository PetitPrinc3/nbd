# Nothing But Data

<div align="center">
    <img src="./images/nbd_logo_icon_flat.png" height="200px"/>
    <h3 align="center">Nothing But Data</h1>
    <p align="center">The No-Bullshit Daemon.</p>
    <a href="https://github.com/PetitPrinc3/nbd/actions/workflows/rust.yml/"><img src="https://github.com/PetitPrinc3/nbd/actions/workflows/rust.yml/badge.svg"/></a>
</div>

---

Nothing but data is a daemon coded in `Rust`and aiming at providing a high performance connector to send multicast messages to a Kafka borker.  
It relies on Zero-Copy and MPSC principles in order to provide a fully optimized system for sensitive production environments.

It complies with the [ANSSI requirements](https://anssi-fr.github.io/rust-guide/) regarding `Rust` application development.

# Features
The software features :
- a high performance data pipeline from multiple multicast groups to a single Kafka broker ;
- a comprehensive configuration file checking utility ;

# Documentation
The documentation of the software is hosted on its [Wiki](https://github.com/PetitPrinc3/nbd/wiki).  

It provides a comprehensive guide to :
* write a coherent configuration file ;
* securely deploy the software ;

As well as :
* a software performance review ;
* an ANSSI requirement conformation matrix ;
