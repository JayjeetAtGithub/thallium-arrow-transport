#include <iostream>

void func(std::shared_ptr<int> &num) {
    std::cout << "func: " << *num << std::endl;
}

int main() {
    std::cout << "Hello World!\n";
    
    std::shared_ptr<int> mynum = std::make_shared<int>(10);


}