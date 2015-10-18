#include <algorithm>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <map>
#include <vector>
#include <random>
#include <cassert>
#include <memory>

const int MaxOperationsToClearFromQueue = 10;
const int MaxOperationsToGenerateAtATime = 10;
const double LikelyhoodOfBeingThrottled = 0.15;
const int MaxOperationsToAcknowledge = 10;
const int UpperPrice = 9;

enum class Action
{
  INSERT_ORDER,
  QUOTE_ONCE,
  QUOTE_TWICE,
  QUOTE_THREE_TIMES,
  QUOTE_FOUR_TIMES,
  QUOTE_FIVE_TIMES,
  QUOTE_SIX_TIMES,
  AMEND_ONCE,
  AMEND_TWICE,
  AMEND_THREE_TIMES,
  DELETE_ORDER,
  DELETE_QUOTE
};

enum class OrderState
{
  PriorToMarket,
  OnMarket,
  DeleteSentToMarket, // delete sent to market
  Finalised // gone
};

enum class OperationType
{
  InsertOrder,
  InsertQuote,
  AmendOrder,
  DeleteOrder,
  DeleteQuote
};

enum class OperationState
{
  Initial,
  Queued,
  SentToMarket,
  Acked
};

struct Order;

struct Operation
{
  Operation(Order& _order)
    : order(_order)
  {
  }

  Order& order;
  Operation* previousOperation = nullptr;
  OperationType operationType;
  OperationState operationState;
  int price;
  int qty;
  int bidPrice;
  int bidQty;
  int askPrice;
  int askQty;
};

enum class Side
{
  Buy,
  Sell
};

struct Order
{
  int price;
  int qty;
  Side side;
  OrderState orderState;
  std::vector<std::unique_ptr<Operation>> operations;
  bool isQuote = false;
};

std::ostream& operator<<(std::ostream& stream, const Operation& operation)
{
  static std::map<OperationType, std::string> typeMap {
    {OperationType::InsertOrder, "InsertOrder"},
    {OperationType::InsertQuote, "InsertQuote"},
    {OperationType::AmendOrder, "AmendOrder"},
    {OperationType::DeleteOrder, "DeleteOrder"},
    {OperationType::DeleteQuote, "DeleteQuote"}
  };
  static std::map<OperationState, std::string> stateMap {
    {OperationState::Initial, "Initial"},
    {OperationState::SentToMarket, "SentToMarket"},
    {OperationState::Queued, "Queued"},
    {OperationState::Acked, "Acked"}
  };

  stream << "Type: " << typeMap[operation.operationType] << ", state: " << stateMap[operation.operationState] << ", ";
  if (operation.order.isQuote)
  {
    stream << operation.bidQty << "@" << operation.bidPrice << "--" << operation.askQty << "@" << operation.askPrice;
  }
  else
  {
    stream << operation.qty << "@" << operation.price;
  }
  return stream;
}

std::ostream& operator<<(std::ostream& stream, const Order& order)
{
  static std::map<OrderState, std::string> stateMap {
    {OrderState::PriorToMarket, "PriorToMarket"},
    {OrderState::OnMarket, "OnMarket"},
    {OrderState::DeleteSentToMarket, "DeleteSentToMarket"},
    {OrderState::Finalised, "Finalised"}
  };

  stream << "State: " << stateMap[order.orderState] << ", Side: " << (order.side == Side::Buy ? "Buy" : "Sell")
         << ", " << order.qty << "@" << order.price << ", operations: ";
  for (auto& operation : order.operations)
    stream << "[ " << *operation.get() << " ]";
  return stream;
}

struct Quote
{
  int buyPrice;
  int buyQty;
  int sellPrice;
  int sellQty;
  std::vector<Operation> operations;
};

std::vector<std::unique_ptr<Order>> orders;
std::vector<Operation*> throttle; // just references to managed objects
// global quote object for order manager (not market book)
Order* quotes;

std::random_device random_device;
std::default_random_engine random_engine(random_device());

template <typename C>
int GetLivePrice(C comparitor, Order& order)
{
  int inflightPrice = order.price;
  int lastAckedPrice = order.price;
  for (auto& operation : order.operations)
  {
    if (operation->operationType == OperationType::AmendOrder || operation->operationType == OperationType::InsertOrder)
    {
      if (operation->operationState == OperationState::Acked)
      {
        // the very latest ack price should be taken into account
        lastAckedPrice = operation->price;
      }
      else
      {
        // take any pending price into account
        inflightPrice = comparitor(operation->price, inflightPrice);
      }
    }
  }
  return comparitor(inflightPrice, lastAckedPrice);
}

bool CheckPendingInsertOrAmend(Order& pendingOrder)
{
  // check quotes first
  if (pendingOrder.side == Side::Buy)
  {
    int lastAckedPrice = std::numeric_limits<int>::max();
    int lowestUnackedPrice = std::numeric_limits<int>::max();
    for (auto& quoteOperation : quotes->operations)
    {
      if (quoteOperation->askQty == -1)
        continue; // no active quote
      if (quoteOperation->operationState == OperationState::Acked)
      {
        lastAckedPrice = quoteOperation->askPrice;
      }
      else
      {
        lowestUnackedPrice = std::min(lowestUnackedPrice, quoteOperation->askPrice);
      }
    }
    int lowestPrice = std::min(lastAckedPrice, lowestUnackedPrice);
    if (pendingOrder.price >= lowestPrice)
    {
      std::cout << "* Buy order crosses with existing quote at price level " << lowestPrice << std::endl;
      return false; // will cross with quote
    }
  }
  else // ask
  {
    int lastAckedPrice = std::numeric_limits<int>::min();
    int highestUnackedPrice = std::numeric_limits<int>::min();
    for (auto& quoteOperation : quotes->operations)
    {
      if (quoteOperation->bidQty == -1)
        continue;
      if (quoteOperation->operationState == OperationState::Acked)
      {
        lastAckedPrice = quoteOperation->bidPrice;
      }
      else
      {
        highestUnackedPrice = std::max(highestUnackedPrice, quoteOperation->bidPrice);
      }
    }
    int highestPrice = std::max(lastAckedPrice, highestUnackedPrice);
    if (pendingOrder.price <= highestPrice)
    {
      std::cout << "* Sell order crosses with existing quote at price level " << highestPrice << std::endl;
      return false; // will cross with quote
    }
  }

  // walk through all opposing orders and check that not in cross
  for (auto& order: orders)
  {
    if (order->side == pendingOrder.side)
      continue; // same order
    if (order->orderState == OrderState::Finalised)
      continue; // can't be in cross if other order is gone
    if (order->orderState == OrderState::DeleteSentToMarket)
      continue; // can't be in cross if other order is going

    if (pendingOrder.side == Side::Buy) // order must be opposing side if we are here
    {
      int pendingBuy = GetLivePrice([](int a, int b) { return std::max(a, b); }, pendingOrder);
      int minSubmittedSell = GetLivePrice([](int a, int b){return std::min(a, b);}, *order.get());
      if (pendingBuy < minSubmittedSell)
        continue;
      else
        std::cout << "* Buy order crosses with existing order" << std::endl;
    }
    else // order must be opposing side if we are here
    {
      int pendingSell = GetLivePrice([](int a, int b) { return std::min(a, b); }, pendingOrder);
      int maxSubmittedBuy = GetLivePrice([](int a, int b){return std::max(a, b);}, *order.get());
      if (pendingSell > maxSubmittedBuy)
        continue;
      else
        std::cout << "* Sell order crosses with existing order" << std::endl;
    }
    return false;
  }
  return true;
}

bool CheckThrottle()
{
  if (!throttle.empty())
    return false; // must throttle
  std::bernoulli_distribution distribution(1 - LikelyhoodOfBeingThrottled); // from time to time simulate window becoming closed
  return distribution(random_engine);
}

void RemoveFromThrottle(Order* order)
{
  throttle.erase(std::remove_if(throttle.begin(), throttle.end(), [order](const Operation* operation)
  {
    if (&operation->order == order)
    {
      std::cout << "Removing operation from throttle: " << *operation << std::endl;
      return true;
    }
    return false;
  }), throttle.end());
}

void RemoveDiscardedOperations(Operation& operation)
{
  auto& operations = operation.order.operations;
  Operation* thisOperation = &operation;
  bool flag = true;
  operations.erase(std::remove_if(operations.begin(), operations.end(), [thisOperation, &flag](const std::unique_ptr<Operation>& ptr)
  {
    if (ptr.get() != thisOperation)
    {
      if (ptr->operationState == OperationState::Queued)
      {
          if (flag)
            thisOperation->previousOperation = ptr->previousOperation;
          flag = false;
          std::cout << "Removing operation from order: " << *ptr << std::endl;
          return true;
      }
    }
    return false;
  }), operations.end());
}

void PushToThrottle(Operation& operation)
{
  // ovewrite anything else in queue (i.e. remove everything else from queue)
  RemoveFromThrottle(&operation.order);
  throttle.push_back(&operation);
  operation.operationState = OperationState::Queued;
  std::cout << "Operation throttled: " << operation << ", queue size now: " << throttle.size() << std::endl;

  // remove discarded throttled operations from order
  RemoveDiscardedOperations(operation);
}

// order book for market
std::vector<Operation*> marketOperations;

void PrintOrderBook()
{
  std::map<int, int> bids;
  std::map<int, int> asks;
  for (Operation* operation : marketOperations)
  {
    if (operation->order.isQuote)
    {
      if (operation->bidQty > -1)
        bids[operation->bidPrice] += operation->bidQty;
      if (operation->askQty > -1)
        asks[operation->askPrice] += operation->askQty;
    }
    else
    {
      if (operation->order.side == Side::Buy)
      {
        bids[operation->price] += operation->qty;
      }
      else
      {
        asks[operation->price] += operation->qty;
      }
    }
  }
  //std::cout << "\033[2J\033[1;1H"; // clear screen
  bool failed = false;
  for (int price = UpperPrice; price > 0; --price)
  {
    std::stringstream bidPrice;
    if (bids[price])
      bidPrice << std::right << std::setfill(' ') << std::setw(5) << bids[price];
    else
      bidPrice << std::right << std::setfill(' ') << std::setw(5) << ' ';
    std::cout << bidPrice.str() << " " << price << " ";
    std::stringstream askPrice;
    if (asks[price])
      askPrice << std::left << std::setfill(' ') << std::setw(5) << asks[price];
    else
      askPrice << std::left << std::setfill(' ') << std::setw(5) << ' ';
    std::cout << askPrice.str() << std::endl;
    if (bids[price] && asks[price])
    {
      std::cout << "********* IN CROSS ************" << std::endl;
      failed = true;
    }
  }
  if (failed)
    exit(-1);
}

void SendToMarket(Operation& operation)
{
  operation.operationState = OperationState::SentToMarket;
  std::cout << "Operation sent to market, " << operation << std::endl;

  if (operation.operationType == OperationType::DeleteOrder || operation.operationType == OperationType::DeleteQuote)
    operation.order.orderState = OrderState::DeleteSentToMarket;
  else
    operation.order.orderState = OrderState::OnMarket;

  // the previous operation overwrites the last
  Operation* previousOperation = operation.previousOperation;
  if (previousOperation)
  {
      auto it = std::find(marketOperations.begin(), marketOperations.end(), previousOperation);
      if (it == marketOperations.end())
      {
        std::cout << "Can't find existing operation in market book: " << *previousOperation << std::endl;
        throw;
      }
      marketOperations.erase(it);
  }
  // add inserts and amends (a delete will have already cleared last item)
  if (operation.operationType == OperationType::InsertOrder || operation.operationType == OperationType::AmendOrder || operation.operationType == OperationType::InsertQuote)
  {
    marketOperations.push_back(&operation); // includes quotes
  }
  PrintOrderBook();
}

int RandomPrice(int lower, int upper)
{
  std::uniform_int_distribution<> distribution(lower, upper);
  return distribution(random_engine);
}

int RandomPrice()
{
  return RandomPrice(1, UpperPrice);
}

int RandomQty()
{
  std::uniform_int_distribution<> distribution(1, 100);
  return distribution(random_engine);
}

Side RandomSide()
{
  std::uniform_int_distribution<> distribution((int)Side::Buy, (int)Side::Sell);
  return (Side)distribution(random_engine);
}

void InsertOrder()
{
  orders.push_back(std::move(std::unique_ptr<Order>(new Order())));
  Order* order = orders.back().get();
  order->price = RandomPrice();
  order->qty = RandomQty();
  order->side = RandomSide();
  order->orderState = OrderState::PriorToMarket;

  order->operations.push_back(std::unique_ptr<Operation>(new Operation(*order)));
  Operation* operation = order->operations.back().get();
  operation->operationType = OperationType::InsertOrder;
  operation->operationState = OperationState::Initial;
  operation->price = order->price;
  operation->qty = order->qty;

  std::cout << "Order insert: " << *order << std::endl;

  if (!CheckPendingInsertOrAmend(*order))
  {
    std::cout << "*** Order insert crossed, rejecting operation: " << *operation << std::endl;
    orders.pop_back();
  }
  else if (!CheckThrottle())
  {
     std::cout << "Throttle closed" << std::endl;
     PushToThrottle(*operation);
  }
  else
  {
    SendToMarket(*operation);
  }
}

Order* GetRandomLiveOrder()
{
  std::uniform_int_distribution<> uniform_dist(0, orders.size());
  const int maxAttempts = orders.size();
  int i = 0;
  while (i++ < maxAttempts)
  {
    int orderIndex = uniform_dist(random_engine);
    auto it = orders.begin() + orderIndex;
    if (it != orders.end())
    {
        Order* order = it->get();
        if (order->orderState == OrderState::OnMarket || order->orderState == OrderState::PriorToMarket)
        {
          if (!order->isQuote)
            return order;
        }
    }
  }
  return nullptr;
}

void DeleteOrder(Order* order)
{
  // mark as deleted (so we don't consider for cross, but still send and wait for ack before removing
  Operation* previousOperation = order->operations.back().get();
  order->operations.push_back(std::unique_ptr<Operation>(new Operation(*order)));
  Operation* operation = order->operations.back().get();
  operation->previousOperation = previousOperation;
  operation->operationType = OperationType::DeleteOrder;
  operation->operationState = OperationState::Initial;
  operation->price = order->price;
  operation->qty = order->qty;
  std::cout << "Order delete, [" << *order << "] , previous operation: " << *previousOperation << std::endl;

  // if order is not live (i.e. queued), can remove right now
  if (order->orderState == OrderState::PriorToMarket)
  {
      RemoveFromThrottle(order);
      order->orderState = OrderState::Finalised;
      auto it = std::find_if(orders.begin(), orders.end(), [order](const std::unique_ptr<Order>& ptr) { return ptr.get() == order; });
      orders.erase(it);
      return;
  }

  // remove any queued items
  RemoveFromThrottle(order);
  // remove discarded throttled operations from order
  RemoveDiscardedOperations(*operation);

  order->orderState = OrderState::DeleteSentToMarket;

  if (!CheckThrottle())
  {
     std::cout << "Throttle closed" << std::endl;
     PushToThrottle(*operation);
  }
  else
  {
    SendToMarket(*operation);
  }
}

void AmendOrder()
{
  // update price/qty of order immediately
  Order* order = GetRandomLiveOrder();
  if (!order)
    return;
  order->price = RandomPrice();
  order->qty = RandomQty();
  Operation* previousOperation = order->operations.back().get();
  order->operations.push_back(std::unique_ptr<Operation>(new Operation(*order)));
  Operation* operation = order->operations.back().get();
  operation->previousOperation = previousOperation;
  operation->operationType = OperationType::AmendOrder;
  operation->operationState = OperationState::Initial;
  operation->price = order->price;
  operation->qty = order->qty;
  std::cout << "Order amend to " << order->qty << "@" << order->price << " [" << *order << "], previous operation: " << *previousOperation << std::endl;

  if (!CheckPendingInsertOrAmend(*order))
  {
    std::cout << "*** Order amend crossed, rejecting operation: " << *operation << std::endl;
    order->operations.pop_back();
    // clear up order (on market and/or in queue)
    DeleteOrder(order);
  }
  else if (!CheckThrottle())
  {
     std::cout << "Throttle closed" << std::endl;
     PushToThrottle(*operation);
  }
  else
  {
    assert(previousOperation->operationState != OperationState::Queued);
    SendToMarket(*operation);
  }
}

void DeleteQuote()
{
  // TODO
  // either throttle or send (but leave global quote object)
}

bool CheckPendingQuote(Operation* quoteOperation)
{
  // we assume that quotes won't cross with each other
  // walk through all orders and check that not in cross
  for (auto& order: orders)
  {
    if (order->isQuote)
      continue; // special quote entry
    if (order->orderState == OrderState::Finalised)
      continue; // can't be in cross if other order is gone
    if (order->orderState == OrderState::DeleteSentToMarket)
      continue; // can't be in cross if other order is going

    if (order->side == Side::Buy)
    {
      if (quoteOperation->askQty > -1)
      {
        int maxSubmittedBuy = GetLivePrice([](int a, int b){return std::max(a, b);}, *order.get());
        if (quoteOperation->askPrice > maxSubmittedBuy)
          continue;
        else
          std::cout << "* Quote ask crosses with existing order" << std::endl;
      }
    }
    else // sell order
    {
      if (quoteOperation->bidQty > -1)
      {
        int minSubmittedSell = GetLivePrice([](int a, int b){return std::min(a, b);}, *order.get());
        if (quoteOperation->bidPrice < minSubmittedSell)
          continue;
        else
          std::cout << "* Quote bid crosses with existing order" << std::endl;
      }
    }
    return false; // the quote crossed with an order
  }
  return true;
}

void InitQuotes()
{
  orders.push_back(std::move(std::unique_ptr<Order>(new Order())));
  Order* order = orders.back().get();
  quotes = order;
  order->isQuote = true;
  order->price = 0;
  order->qty = -1;
  order->side = RandomSide(); // not important here
  order->orderState = OrderState::PriorToMarket;
}

void Quote()
{
  // A quote as just another order that stays alive and is two sided. So we need
  // to check all outstanding quote operations, not just current (due to throttling)
  Operation* previousOperation = nullptr;
  if (!quotes->operations.empty())
  {
     previousOperation = quotes->operations.back().get();
  }
  quotes->operations.push_back(std::unique_ptr<Operation>(new Operation(*quotes)));
  Operation* operation = quotes->operations.back().get();
  operation->operationState = OperationState::Initial;
  operation->operationType = OperationType::InsertQuote;
  operation->previousOperation = previousOperation;
  operation->bidPrice = RandomPrice(1, UpperPrice - 1);
  operation->bidQty = RandomQty();
  operation->askPrice = RandomPrice(operation->bidPrice + 1, UpperPrice);
  operation->askQty = RandomQty();

  std::cout << "Quote insert: " << *operation << std::endl;

  // check that quote isn't in cross. If it is, delete previous quote
  if (!CheckPendingQuote(operation))
  {
    std::cout << "*** Quote insert crossed, rejecting operation: " << *operation << std::endl;
    quotes->operations.pop_back();
    return;
  }

  if (!CheckThrottle())
  {
     std::cout << "Throttle closed" << std::endl;
    // add to throttle and conflate any other quote operations (including deletes)
    PushToThrottle(*operation);
    return;
  }
  SendToMarket(*operation);
}

void PerformAction(Action action)
{
  switch (action)
  {
    case Action::INSERT_ORDER:
      InsertOrder();
      break;
    case Action::DELETE_ORDER:
      {
      Order* order = GetRandomLiveOrder();
        if (order)
          DeleteOrder(order);
      }
      break;
    case Action::AMEND_ONCE:
    case Action::AMEND_TWICE:
    case Action::AMEND_THREE_TIMES:
      AmendOrder();
      break;
    case Action::QUOTE_ONCE:
    case Action::QUOTE_TWICE:
    case Action::QUOTE_THREE_TIMES:
    case Action::QUOTE_FOUR_TIMES:
    case Action::QUOTE_FIVE_TIMES:
    case Action::QUOTE_SIX_TIMES:
      Quote();
      break;
    case Action::DELETE_QUOTE:
      DeleteQuote();
      break;
  }
}

void GenerateOrderOperations()
{
  std::uniform_int_distribution<> numOpsGenerator(1, MaxOperationsToGenerateAtATime);
  int numOperations = numOpsGenerator(random_engine);

  std::uniform_int_distribution<> uniform_dist((int)Action::INSERT_ORDER, (int)Action::DELETE_QUOTE);
  for (int i = 0; i < numOperations; ++i)
  {
    Action action = (Action)uniform_dist(random_engine);
    PerformAction(action);
  }
}

void AckOrderOperations()
{
  // TODO randomise a little
  std::uniform_int_distribution<> distribution(0, MaxOperationsToAcknowledge);
  int numItemsToAck = distribution(random_engine);
  int itemsAcked = 0;
  for (auto& order : orders)
  {
    if (order->orderState == OrderState::Finalised)
      continue;
    for (auto& operation : order->operations)
    {
      if (itemsAcked == numItemsToAck)
        break;
      if (operation->operationState == OperationState::SentToMarket)
      {
        std::cout << "Acked operation " << *operation.get() << std::endl;
        operation->operationState = OperationState::Acked;
        if (operation->operationType == OperationType::DeleteOrder)
        {
          order->orderState = OrderState::Finalised;
        }
        else
        {
          // only mark as on market if we haven't already marked this as deleting
          if (order->orderState != OrderState::DeleteSentToMarket)
            order->orderState = OrderState::OnMarket;
        }

        ++itemsAcked;
      }
    }
  }
}

void ProcessThrottleQueue()
{
  if (throttle.empty())
    return;

  std::cout << "Throttle queue contains: ";
  for (auto& operation : throttle)
    std::cout << *operation;
  std::cout << std::endl;

  std::uniform_int_distribution<> distribution(0, MaxOperationsToClearFromQueue);
  int window = distribution(random_engine);
  // deletes first
  std::vector<Operation*>::reverse_iterator it = throttle.rbegin();
  while (window > 0 && it != throttle.rend())
  {
    Operation& operation = **it;
    ++it;
    if (operation.operationType == OperationType::DeleteOrder || operation.operationType == OperationType::DeleteQuote)
    {
      std::cout << "Operation popped from throttle, " << operation << std::endl;
      SendToMarket(operation);
      it = std::vector<Operation*>::reverse_iterator(throttle.erase(it.base()));
      --window;
      continue;
    }
  }
  // all other operations
  it = throttle.rbegin();
  while (window > 0 && it != throttle.rend())
  {
    Operation& operation = **it;
    ++it;
    if (operation.operationType != OperationType::DeleteOrder && operation.operationType != OperationType::DeleteQuote)
    {
      std::cout << "Operation popped from throttle, " << operation << std::endl;
      SendToMarket(operation);
      it = std::vector<Operation*>::reverse_iterator(throttle.erase(it.base()));
      --window;
    }
  }
}

int main()
{
  InitQuotes();
  while (true)
  {
    GenerateOrderOperations();
    ProcessThrottleQueue();
    AckOrderOperations();

    // only clear memory once and a while
    if (orders.size() > 1000)
    {
      orders.erase(std::remove_if(orders.begin(), orders.end(), [](const std::unique_ptr<Order>& ptr) { return ptr->orderState == OrderState::Finalised; }), orders.end());
      std::cout << "CLEARING ORDERS" << std::endl;
    }

    // just remove most of the acked quotes, if any of the remainder are already acked
    if (quotes->operations.size() > 200)
    {
      if (quotes->operations[150]->operationState == OperationState::Acked)
      {
        quotes->operations.erase(quotes->operations.begin(), quotes->operations.begin() + 150);
        std::cout << "CLEARING QUOTES" << std::endl;
      }
    }
  }
}

