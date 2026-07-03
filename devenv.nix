{ pkgs, lib, config, inputs, ... }:

{
  packages = with pkgs; [ git libyaml openssl zig ];

  languages.rust = {
    enable = true;
    channel = "nightly";
    targets = [ "x86_64-unknown-linux-musl" ];
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer"];
  };

  enterShell = ''

  '';
}
