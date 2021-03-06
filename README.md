DMV: Distributed Media Versioning -- Source Code
==================================================

DMV is a project to generalize version control beyond source code and into
larger files such as photos and video, and also into larger collections that
might not fit on one disk. It hopes to be a cross between a version control
system and a generalized distributed data store.


Source code
==================================================

This repository contains the DMV prototype source code.

Note: I settled on the name after developing the prototype, so in older versions
the source code, the crate and the executable are still named `prototype`.


Compiling
--------------------------------------------------

DMV is written in Rust and is a Cargo library+binary crate. It is not published
in the Rust crate registry yet, but with the source downloaded it can be built
easily enough with the standard Cargo targets:

- `cargo build` -- Build the lib and executable
- `cargo test` -- Run unit tests
- `cargo doc` -- Build crate documentation
- `cargo doc --open` -- Launch documentation in browser

The DMV prototype was developed under Rust 1.16 stable, and should compile with
no trouble with stable Rust on Linux.

Note that for much of development, this repository and the DMV Test Code
repository were combined, with the prototype code in the `prototype`
subdirectory of the repository. So if you check out old versions you may have to
`cd` to the `prototype` subdirectory to find the Rust project again.


Running
--------------------------------------------------

The DMV prototype handles a lot like any command-line version control system you
might be used to. Run `prototype help` for a list of commands which should be
familiar to any heavy Git user.


More About DMV
==================================================

DMV hopes to extend the distributed part of the distributed version control
concept so that the actual collection/history can be distributed across several
repositories, making it easy to transfer the files you need to the locations
where you need them and to keep everything synchronized.

DMV was created as a master's thesis project at the University of Tromsø,
Norway's Arctic University, by a student named Mike Murphy (that's me). The
prototype is definitely not ready for prime time yet, but I do think I'm on to
something here.


Documentation and other related repositories
--------------------------------------------------

At this point the best source of documentation for the project is the master's
thesis itself. An archived PDF version of the thesis is available in Munin, the
University of Tromsø's open research archive
(<http://hdl.handle.net/10037/11213>).

Beyond that there are three source repositories of interest:

1. [DMV Source Code]( https://github.com/sleepymurph/dmv), the prototype source
   code itself.
2. [DMV Publications]( https://github.com/sleepymurph/dmv-publications), LaTeX
   and other materials used to generate publication PDFs, including the master's
   thesis itself and presentation slides. Also includes experiment data.
3. [DMV Test Code]( https://github.com/sleepymurph/dmv-test-code), including
   helpers scripts used in my research and experiment/benchmark scripts.

I welcome any feedback or questions at <dmv@sleepymurph.com>.
