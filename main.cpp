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

  stream << "Type: " << typeMap[operation.operationType] << ", state: " << stateMap[operation.operationState]
         << ", " << operation.qty << "@" << operation.price;
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
std::vector<std::unique_ptr<Quote>> quotes;
std::vector<Operation*> throttle; // just references to managed objects
int lastQuoteBidQty = -1;
int lastQuoteBidPrice = -1;
int lastQuoteAskQty = -1;
int lastQuoteAskPrice = -1;

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
    if (lastQuoteAskQty > -1)
    {
      int pendingBuyPrice = GetLivePrice([](int a, int b) { return std::max(a, b); }, pendingOrder);
      if (pendingBuyPrice >= lastQuoteAskQty)
      {
        return false; // will cross with quote
      }
    }
  }
  else // ask
  {
    if (lastQuoteBidQty > -1)
    {
      int pendingAskPrice = GetLivePrice([](int a, int b) { return std::min(a, b); }, pendingOrder);
      if (pendingAskPrice <= lastQuoteBidQty)
      {
        return false; // will cross with quote
      }
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
    }
    else // order must be opposing side if we are here
    {
      int pendingSell = GetLivePrice([](int a, int b) { return std::min(a, b); }, pendingOrder);
      int maxSubmittedBuy = GetLivePrice([](int a, int b){return std::max(a, b);}, *order.get());
      if (pendingSell > maxSubmittedBuy)
        continue;
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

std::vector<Operation*> marketOrders;

void PrintOrderBook()
{
  std::map<int, int> bids;
  std::map<int, int> asks;
  for (Operation* operation : marketOrders)
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
    throw;
}

void SendToMarket(Operation& operation)
{
  operation.operationState = OperationState::SentToMarket;
  std::cout << "Operation sent to market, " << operation << std::endl;

  if (operation.operationType == OperationType::DeleteOrder)
    operation.order.orderState = OrderState::DeleteSentToMarket;
  else
    operation.order.orderState = OrderState::OnMarket;

  // the previous operation overwrites the last
  Operation* previousOperation = operation.previousOperation;
  if (previousOperation)
  {
      auto it = std::find(marketOrders.begin(), marketOrders.end(), previousOperation);
      if (it == marketOrders.end())
      {
        std::cout << "Can't find existing operation in market book: " << *previousOperation << std::endl;
        throw;
      }
      marketOrders.erase(it);
  }
  // add inserts and amends (a delete will have already cleared last item)
  if (operation.operationType == OperationType::InsertOrder || operation.operationType == OperationType::AmendOrder)
  {
    marketOrders.push_back(&operation);
  }
  PrintOrderBook();
}

int RandomPrice()
{
  std::uniform_int_distribution<> distribution(1, UpperPrice);
  return distribution(random_engine);
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
          return order;
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
     PushToThrottle(*operation);
  }
  else
  {
    assert(previousOperation->operationState != OperationState::Queued);
    SendToMarket(*operation);
  }
}

void Quote()
{
  // TODO
  // update global quote state immediately, but still send operations
  // (i.e. check for cross, then either throttle or send)
}

void DeleteQuote()
{
  // TODO
  // update global state
  // either throttle or send
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
  while (true)
  {
    GenerateOrderOperations();
    ProcessThrottleQueue();
    AckOrderOperations();

    // only clear memory once and a while
    if (orders.size() > 1000)
      orders.erase(std::remove_if(orders.begin(), orders.end(), [](const std::unique_ptr<Order>& ptr) { return ptr->orderState == OrderState::Finalised; }), orders.end());
  }
}

