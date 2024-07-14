# Made by the example from: https://nixos.wiki/wiki/Rust#Installation_via_rustup
{
  description = "Nix flake for Xline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };
  outputs =
    { self, nixpkgs, ... }:
    let
      # system should match the system you are running on
      system = "x86_64-linux";
      overrides = (builtins.fromTOML (builtins.readFile ./rust-toolchain.toml));
      pkgs = import nixpkgs { inherit system; };
      # load external libraries that you need in your rust project here
      libPath = with pkgs; lib.makeLibraryPath [ ];
    in
    {
      devShells."${system}".default = pkgs.mkShell {
        packages = with pkgs; [
          clang
          llvmPackages.bintools
          rustup
          sccache
        ];
        RUSTC_VERSION = overrides.toolchain.channel;
        LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages_latest.libclang.lib ];
        shellHook = ''
          export PATH=$PATH:''${CARGO_HOME:-~/.cargo}/bin
          export PATH=$PATH:''${RUSTUP_HOME:-~/.rustup}/toolchains/$RUSTC_VERSION-x86_64-unknown-linux-gnu/bin/
          export PROTOC="${pkgs.protobuf_27}/bin/protoc"
          export RUSTC_WRAPPER=sccache
          export SCCACHE_CACHE_SIZE=50G
        '';
        # Add precompiled library to rustc search path
        RUSTFLAGS = (
          builtins.map (a: ''-L ${a}/lib'') [
            # add libraries here (e.g. pkgs.libvmi)
          ]
        );
        LD_LIBRARY_PATH = libPath;
        # Add glibc, clang, glib, and other headers to bindgen search path
        BINDGEN_EXTRA_CLANG_ARGS =
          # Includes normal include path
          (builtins.map (a: ''-I"${a}/include"'') [
            # add dev libraries here (e.g. pkgs.libvmi.dev)
            pkgs.glibc.dev
          ])
          # Includes with special directory paths
          ++ [
            ''-I"${pkgs.llvmPackages_latest.libclang.lib}/lib/clang/${pkgs.llvmPackages_latest.libclang.version}/include"''
            ''-I"${pkgs.glib.dev}/include/glib-2.0"''
            ''-I${pkgs.glib.out}/lib/glib-2.0/include/''
          ];
      };
    };
}
