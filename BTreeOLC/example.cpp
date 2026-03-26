#include <iostream>
#include <string>
#include <vector>

#include "BTreeOLC_child_layout.h"

using Tree_t = btreeolc::BTree<std::string, std::string>;

int main() {
  Tree_t tree;
  int nkey = 1000;
  std::vector<std::string> keys;

  for (int i = 0; i < nkey; i++) {
    keys.push_back(std::to_string(i));
  }

  for (auto &key : keys) {
    tree.insert(key, key);
  }

  for (auto &key : keys) {
    std::string value;
    bool find = tree.lookup(key, value);
    if (!find) {
      std::cout << "error not found: " << key << std::endl;
    } else if (value != key) {
      std::cout << "error error value: " << key << " " << value << std::endl;
    } else {
      std::cout << "found: " << key << " " << value << std::endl;
    }
  }

  std::string output[200];
  auto scanned = tree.scan("1", 200, output);
  std::cout << "scanned: " << scanned << std::endl;

  return 0;
}