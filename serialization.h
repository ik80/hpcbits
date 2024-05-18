#pragma once
#include <array>
#include <cstdint>  // uint64_t
#include <cstring>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

// TODO: namespace
// TODO: patch serialization object type hash into protocol header
// TODO: or just add static hash definition/calculation to existing
// SERIALIZATION_FRIENDS
// TODO: or just use https://github.com/Manu343726/ctti
#ifndef DYNA_SERIALIZATION_PACKET_HEADER_RESERVED     // When serializing you'll
                                                      // get set of buffers with
                                                      // this much reserved
#define DYNA_SERIALIZATION_PACKET_HEADER_RESERVED 128  // This MUST MATCH the (fixed) size of your headers on the wire
#endif
#ifndef DYNA_SERIALIZATION_FRAGMENT_SIZE
#define DYNA_SERIALIZATION_FRAGMENT_SIZE 1344  // "Internet MTU"
#endif
using FixedBuffer = std::pair<std::array<char, DYNA_SERIALIZATION_FRAGMENT_SIZE>, size_t>;
using FixedBufferPtr = std::shared_ptr<FixedBuffer>;
// Goal for this was fastest possible serialization that also doesnt look too
// ugly It has two modes (datagram/stream oriented)
//
// Datagram mode uses scatter gather operation during de/serialization
// When serializing, boundaryCounter is interleaved with fragment data in teh
// big TX buffer while simultaneously accumulating iovecs for later use in
// sendmmsg When deserializing from multiple iovecs, boudnary counter is at the
// beginning of each iovec buffer
//
// Stream mode is straightforward
// Usage: See example at the end of file
struct SerializationContext
{
  SerializationContext()
  {
    FixedBufferPtr nextFragment(new FixedBuffer());
    buffers.push_back(nextFragment);  // push completed iovec
  };
  ~SerializationContext() = default;
  SerializationContext(const SerializationContext&) = default;
  SerializationContext(SerializationContext&&) = default;
  SerializationContext& operator=(const SerializationContext&) = default;
  SerializationContext& operator=(SerializationContext&&) = default;
  uint32_t offset = sizeof(int32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED;
  uint32_t boundaryCounter = 0;
  std::deque<FixedBufferPtr> buffers;  // TODO: buffers should be pooled
  inline void GetNextFragmentIfNeeded()
  {
    if (boundaryCounter >= *((uint32_t*)(buffers.back()->first.data() + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED)))
    {
      offset = sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED;
      buffers.pop_back();
    }
  }
  inline char* FragmentBuffer() const { return &(buffers.back()->first[offset]); }
  inline void AdvanceOffset(uint32_t numBytes)
  {
    offset += numBytes;
    ++boundaryCounter;
  }
  inline bool EnoughSpaceFor(uint32_t size) const { return DYNA_SERIALIZATION_FRAGMENT_SIZE - offset >= size; }
  inline uint32_t SpaceLeftOnFragment() const { return DYNA_SERIALIZATION_FRAGMENT_SIZE - offset; }
  inline void CompleteFragment()
  {
    // patch boundaryCounter to the start of the fragment
    *(reinterpret_cast<uint32_t*>(&(buffers.back()->first[DYNA_SERIALIZATION_PACKET_HEADER_RESERVED]))) = boundaryCounter;
    buffers.back()->second = offset;  // saving size for send
    FixedBufferPtr nextFragment(new FixedBuffer());
    buffers.push_back(nextFragment);  // push completed iovec
    offset = sizeof(int32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED;
  }
  inline void CompleteContext()
  {
    // patch boundaryCounter to the start of the fragment
    *(reinterpret_cast<uint32_t*>(&(buffers.back()->first[DYNA_SERIALIZATION_PACKET_HEADER_RESERVED]))) = boundaryCounter;
    buffers.back()->second = offset;  // saving size for send
  }
};
// forward declaration macro
#define SERIALIZATION_FORWARDS(TYPENAME)                                                     \
  template <>                                                                                \
  void Serialize<TYPENAME>(TYPENAME & message, SerializationContext * pContext);             \
  template <>                                                                                \
  void DeSerialize<TYPENAME>(TYPENAME & message, SerializationContext * pContext);           \
  template <>                                                                                \
  void GetSerializeableLength<TYPENAME>(TYPENAME & message, uint64_t & messageSize);         \
  template <>                                                                                \
  void SerializeBuffer<TYPENAME>(TYPENAME & message, char*& pBuffer, uint64_t& messageSize); \
  template <>                                                                                \
  void DeSerializeBuffer<TYPENAME>(TYPENAME & message, char*& pBuffer);
// friend delcarations macro
#define SERIALIZATION_FRIENDS(TYPENAME)                                                             \
  friend void Serialize<TYPENAME>(TYPENAME & message, SerializationContext * pContext);             \
  friend void DeSerialize<TYPENAME>(TYPENAME & message, SerializationContext * pContext);           \
  friend void GetSerializeableLength<TYPENAME>(TYPENAME & message, uint64_t & messageSize);         \
  friend void SerializeBuffer<TYPENAME>(TYPENAME & message, char*& pBuffer, uint64_t& messageSize); \
  friend void DeSerializeBuffer<TYPENAME>(TYPENAME & message, char*& pBuffer);
// TODO: type name hash generation can go here
// macro to force templated functions instantination, this was needed by some
// old version of gcc in some case, tests compile and run fine now without
#define SERIALIZATION_FORCE_INSTANTINATION(TYPENAME)       \
  void DummySerializationInstantination##TYPENAME()        \
  {                                                        \
    char* pBuffer = nullptr;                               \
    TYPENAME dummyObj;                                     \
    uint64_t dummySize;                                    \
    SerializationContext* pSerializationContext = nullptr; \
    Serialize(dummyObj, pSerializationContext);            \
    DeSerialize(dummyObj, pSerializationContext);          \
    GetSerializeableLength(dummyObj, dummySize);           \
    SerializeBuffer(dummyObj, pBuffer, dummySize);         \
    DeSerializeBuffer(dummyObj, pBuffer);                  \
  }
// Basic templated versions
template <typename S>
void Serialize(S& message, SerializationContext* pContext)
{
  static_assert(std::is_pod<S>());  // non pod structure missing serialization
                                    // methods specializations
  static_assert(sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <=
                DYNA_SERIALIZATION_FRAGMENT_SIZE);  // pod structure + boudnaryCounter
                                                    // size dont fit into UDP packet
  if (!pContext->EnoughSpaceFor(sizeof(S))) pContext->CompleteFragment();
  *(reinterpret_cast<S*>(pContext->FragmentBuffer())) = message;  // BEWARE ALIASING
  pContext->AdvanceOffset(sizeof(S));
}
template <typename S>
void DeSerialize(S& message,
                 SerializationContext* pContext)  // pContext should have offset == 0,
                                                  // boundaryCounter == 0 and have iovecs ready
{
  static_assert(std::is_pod<S>());  // non pod structure missing serialization
                                    // methods specializations
  pContext->GetNextFragmentIfNeeded();
  message = *(reinterpret_cast<S*>(pContext->FragmentBuffer()));
  pContext->AdvanceOffset(sizeof(S));
}
template <typename S>
void GetSerializeableLength(S& message, uint64_t& messageSize)
{
  static_assert(std::is_pod<S>());  // non pod structure missing serialization
                                    // methods specializations
  messageSize += sizeof(S);
}
template <typename S>
void SerializeBuffer(S& message, char*& pBuffer, uint64_t& messageSize)
{
  *(reinterpret_cast<S*>(pBuffer)) = message;
  pBuffer += sizeof(S);
  messageSize += sizeof(S);
}
template <typename S>
void DeSerializeBuffer(S& message, char*& pBuffer)
{
  message = *(reinterpret_cast<S*>(pBuffer));
  pBuffer += sizeof(S);
}
// Template specializations for collections. feel free to roll your own
// std::vector
template <typename S>
void Serialize(std::vector<S>& messageVector, SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    // serialize stuff in chunks with size slightly less than
    // DYNA_SERIALIZATION_FRAGMENT_SIZE and handl it with few memcopy calls
    // layout:
    // <int_totalelements>[<int_elementsthischunk><data>]*
    const size_t ELEMENTS_PER_FRAGMENT = (DYNA_SERIALIZATION_FRAGMENT_SIZE - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - 2 * sizeof(uint64_t)) / sizeof(S);
    size_t elementsToGo = messageVector.size();
    size_t counter = 0;
    Serialize(elementsToGo, pContext);
    // fill up the remainder of the fragment first;
    if (elementsToGo)
    {
      size_t spaceLeftOnFragment = pContext->SpaceLeftOnFragment();
      size_t elementsThisFragment = (spaceLeftOnFragment - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - sizeof(uint64_t)) / sizeof(S);
      if (elementsThisFragment)
      {
        Serialize(elementsThisFragment, pContext);
        std::memcpy(pContext->FragmentBuffer(), &messageVector[counter], elementsThisFragment * sizeof(S));
        pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
        pContext->CompleteFragment();
        counter += elementsThisFragment;
        elementsToGo -= elementsThisFragment;
        // stamp out full fragments with the contents of vector
      }
      else
        pContext->CompleteFragment();
      while (elementsToGo >= ELEMENTS_PER_FRAGMENT)
      {
        elementsThisFragment = ELEMENTS_PER_FRAGMENT;
        Serialize(elementsThisFragment, pContext);
        std::memcpy(pContext->FragmentBuffer(), &messageVector[counter], elementsThisFragment * sizeof(S));
        pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
        pContext->CompleteFragment();
        counter += elementsThisFragment;
        elementsToGo -= elementsThisFragment;
      }
      // write out leftovers if any, dont complete fragments just yet
      if (elementsToGo)
      {
        Serialize(elementsToGo, pContext);
        std::memcpy(pContext->FragmentBuffer(), &messageVector[counter], elementsToGo * sizeof(S));
        pContext->AdvanceOffset(elementsToGo * sizeof(S));
      }
    }
  }
  else
  {
    uint64_t vectorSize = messageVector.size();
    Serialize(vectorSize, pContext);
    for (typename std::vector<S>::iterator it = messageVector.begin(); it != messageVector.end(); ++it) Serialize(*it, pContext);
  }
}
template <typename S>
void DeSerialize(std::vector<S>& messageVector, SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    uint64_t vectorSize = 0;
    DeSerialize(vectorSize, pContext);
    messageVector.resize(vectorSize);
    size_t counter = 0;
    while (vectorSize)
    {
      uint64_t elementsThisFragment = 0;
      DeSerialize(elementsThisFragment, pContext);
      std::memcpy(&messageVector[counter], pContext->FragmentBuffer(), elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      counter += elementsThisFragment;
      vectorSize -= elementsThisFragment;
    }
  }
  else
  {
    uint64_t vectorSize = 0;
    DeSerialize(vectorSize, pContext);
    messageVector.resize(vectorSize);
    for (uint64_t i = 0; i < vectorSize; ++i) DeSerialize(messageVector[i], pContext);
  }
}
template <typename S>
void GetSerializeableLength(std::vector<S>& messageVector, uint64_t& messageSize)
{
  // TODO: HERE, if ((std::is_pod<S>::value == true) && (sizeof(S) +
  // sizeof(uint32_t) < DYNA_SERIALIZATION_FRAGMENT_SIZE)) , serialize stuff in
  // chunks with size slightly less than DYNA_SERIALIZATION_FRAGMENT_SIZE and
  // handl it with few memcopy calls
  messageSize += sizeof(uint64_t);
  for (uint64_t i = 0; i < messageVector.size(); ++i) GetSerializeableLength(messageVector[i], messageSize);
}
template <typename S>
void SerializeBuffer(std::vector<S>& messageVector, char*& pBuffer, uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true)
  {
    uint64_t vectorSize = messageVector.size();
    SerializeBuffer(vectorSize, pBuffer, messageSize);
    std::memcpy(pBuffer, &messageVector[0], sizeof(S) * vectorSize);
    pBuffer += sizeof(S) * vectorSize;
    messageSize += sizeof(S) * vectorSize;
  }
  else
  {
    uint64_t vectorSize = messageVector.size();
    SerializeBuffer(vectorSize, pBuffer, messageSize);
    for (typename std::vector<S>::iterator it = messageVector.begin(); it != messageVector.end(); ++it) SerializeBuffer(*it, pBuffer, messageSize);
  }
}
template <typename S>
void DeSerializeBuffer(std::vector<S>& messageVector, char*& pBuffer)
{
  // let users clear their own structures
  // messageVector.clear();
  if (std::is_pod<S>::value == true)
  {
    uint64_t vectorSize = 0;
    DeSerializeBuffer(vectorSize, pBuffer);
    messageVector.resize(vectorSize);
    std::memcpy(&messageVector[0], pBuffer, sizeof(S) * vectorSize);
    pBuffer += sizeof(S) * vectorSize;
  }
  else
  {
    uint64_t vectorSize = 0;
    DeSerializeBuffer(vectorSize, pBuffer);
    messageVector.resize(vectorSize);
    for (uint64_t i = 0; i < vectorSize; ++i) DeSerializeBuffer(messageVector[i], pBuffer);
  }
}
// std::array
// TODO: fix this shit
template <typename S, std::size_t N>
void Serialize(std::array<S, N>& messageArray, SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    // serialize stuff in chunks with size slightly less than
    // DYNA_SERIALIZATION_FRAGMENT_SIZE and handl it with few memcopy calls
    // layout:
    // [<int_elementsthischunk><data>]*
    const size_t ELEMENTS_PER_FRAGMENT = (DYNA_SERIALIZATION_FRAGMENT_SIZE - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - 2 * sizeof(uint64_t)) / sizeof(S);
    size_t elementsToGo = N;
    size_t counter = 0;
    // fill up the remainder of the fragment first;
    size_t spaceLeftOnFragment = pContext->SpaceLeftOnFragment();
    size_t elementsThisFragment = (spaceLeftOnFragment - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - sizeof(uint64_t)) / sizeof(S);
    if (elementsThisFragment)
    {
      Serialize(elementsThisFragment, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      pContext->CompleteFragment();
      counter += elementsThisFragment;
      elementsToGo -= elementsThisFragment;
      // stamp out full fragments with the contents of vector
    }
    else
      pContext->CompleteFragment();
    while (elementsToGo >= ELEMENTS_PER_FRAGMENT)
    {
      elementsThisFragment = ELEMENTS_PER_FRAGMENT;
      Serialize(elementsThisFragment, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      pContext->CompleteFragment();
      counter += elementsThisFragment;
      elementsToGo -= elementsThisFragment;
    }
    // write out leftovers if any, dont complete fragments just yet
    if (elementsToGo)
    {
      Serialize(elementsToGo, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsToGo * sizeof(S));
      pContext->AdvanceOffset(elementsToGo * sizeof(S));
    }
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) Serialize(*it, pContext);
  }
}
template <typename S, std::size_t N>
void DeSerialize(std::array<S, N>& messageArray, SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    size_t arraySize = N;
    size_t counter = 0;
    while (arraySize)
    {
      uint64_t elementsThisFragment = 0;
      DeSerialize(elementsThisFragment, pContext);
      std::memcpy(&messageArray[counter], pContext->FragmentBuffer(), elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      counter += elementsThisFragment;
      arraySize -= elementsThisFragment;
    }
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) DeSerialize(*it, pContext);
  }
}
template <typename S, std::size_t N>
void GetSerializeableLength(std::array<S, N>& messageArray, uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    messageSize += N * sizeof(S);
  }
  else
    for (uint64_t i = 0; i < N; ++i) GetSerializeableLength(messageArray[i], messageSize);
}
template <typename S, std::size_t N>
void SerializeBuffer(std::array<S, N>& messageArray, char*& pBuffer, uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true)
  {
    std::memcpy(pBuffer, &messageArray[0], sizeof(S) * messageArray.size());
    pBuffer += sizeof(S) * messageArray.size();
    messageSize += sizeof(S) * messageArray.size();
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) SerializeBuffer(*it, pBuffer, messageSize);
  }
}
template <typename S, std::size_t N>
void DeSerializeBuffer(std::array<S, N>& messageArray, char*& pBuffer)
{
  // let users clear their own structures
  if (std::is_pod<S>::value == true)
  {
    std::memcpy(&messageArray[0], pBuffer, sizeof(S) * messageArray.size());
    pBuffer += sizeof(S) * messageArray.size();
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) DeSerializeBuffer(*it, pBuffer);
  }
}
// fixed size array
template <typename S, int N>
void Serialize(S (&messageArray)[N], SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    // serialize stuff in chunks with size slightly less than
    // DYNA_SERIALIZATION_FRAGMENT_SIZE and handl it with few memcopy calls
    // layout:
    // [<int_elementsthischunk><data>]*
    const size_t ELEMENTS_PER_FRAGMENT = (DYNA_SERIALIZATION_FRAGMENT_SIZE - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - 2 * sizeof(uint64_t)) / sizeof(S);
    size_t elementsToGo = N;
    size_t counter = 0;
    // fill up the remainder of the fragment first;
    size_t spaceLeftOnFragment = pContext->SpaceLeftOnFragment();
    size_t elementsThisFragment = (spaceLeftOnFragment - DYNA_SERIALIZATION_PACKET_HEADER_RESERVED - sizeof(uint64_t)) / sizeof(S);
    if (elementsThisFragment)
    {
      Serialize(elementsThisFragment, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      pContext->CompleteFragment();
      counter += elementsThisFragment;
      elementsToGo -= elementsThisFragment;
      // stamp out full fragments with the contents of vector
    }
    else
      pContext->CompleteFragment();
    while (elementsToGo >= ELEMENTS_PER_FRAGMENT)
    {
      elementsThisFragment = ELEMENTS_PER_FRAGMENT;
      Serialize(elementsThisFragment, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      pContext->CompleteFragment();
      counter += elementsThisFragment;
      elementsToGo -= elementsThisFragment;
    }
    // write out leftovers if any, dont complete fragments just yet
    if (elementsToGo)
    {
      Serialize(elementsToGo, pContext);
      std::memcpy(pContext->FragmentBuffer(), &messageArray[counter], elementsToGo * sizeof(S));
      pContext->AdvanceOffset(elementsToGo * sizeof(S));
    }
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) Serialize(*it, pContext);
  }
}
template <typename S, int N>
void DeSerialize(S (&messageArray)[N], SerializationContext* pContext)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    size_t arraySize = N;
    size_t counter = 0;
    while (arraySize)
    {
      uint64_t elementsThisFragment = 0;
      DeSerialize(elementsThisFragment, pContext);
      std::memcpy(&messageArray[counter], pContext->FragmentBuffer(), elementsThisFragment * sizeof(S));
      pContext->AdvanceOffset(elementsThisFragment * sizeof(S));
      counter += elementsThisFragment;
      arraySize -= elementsThisFragment;
    }
  }
  else
  {
    for (typename std::array<S, N>::iterator it = messageArray.begin(); it != messageArray.end(); ++it) DeSerialize(*it, pContext);
  }
}
template <typename S, int N>
void GetSerializeableLength(S (&messageArray)[N], uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true && (sizeof(S) + sizeof(uint32_t) + DYNA_SERIALIZATION_PACKET_HEADER_RESERVED <= DYNA_SERIALIZATION_FRAGMENT_SIZE))
  {
    messageSize += N * sizeof(S);
  }
  else
    for (uint64_t i = 0; i < N; ++i) GetSerializeableLength(messageArray[i], messageSize);
}
template <typename S, int N>
void SerializeBuffer(S (&messageArray)[N], char*& pBuffer, uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true)
  {
    std::memcpy(pBuffer, messageArray, sizeof(S) * N);
    pBuffer += sizeof(S) * N;
    messageSize += sizeof(S) * N;
  }
  else
    for (uint64_t i = 0; i < N; ++i) SerializeBuffer(messageArray[i], pBuffer, messageSize);
}
template <typename S, int N>
void DeSerializeBuffer(S (&messageArray)[N], char*& pBuffer)
{
  if (std::is_pod<S>::value == true)
  {
    std::memcpy(messageArray, pBuffer, sizeof(S) * N);
    pBuffer += sizeof(S) * N;
  }
  else
    for (uint64_t i = 0; i < N; ++i) DeSerializeBuffer(messageArray[i], pBuffer);
}
// dynamically allocated array
template <typename S>
void Serialize(S* messageArray, uint64_t& numElements, SerializationContext* pContext)
{
  // TODO: HERE, if ((std::is_pod<S>::value == true) && (sizeof(S) +
  // sizeof(uint32_t) < DYNA_SERIALIZATION_FRAGMENT_SIZE)) , serialize stuff in
  // chunks with size slightly less than DYNA_SERIALIZATION_FRAGMENT_SIZE and
  // handl it with few memcopy calls
  Serialize(numElements, pContext);
  S(*messageArrayFixedSize)[numElements] = (S(*)[numElements])messageArray;
  Serialize(messageArrayFixedSize, pContext);
}
template <typename S>
void DeSerialize(S* messageArray, uint64_t& numElements, SerializationContext* pContext)
{
  // TODO: HERE, if ((std::is_pod<S>::value == true) && (sizeof(S) +
  // sizeof(uint32_t) < DYNA_SERIALIZATION_FRAGMENT_SIZE)) , serialize stuff in
  // chunks with size slightly less than DYNA_SERIALIZATION_FRAGMENT_SIZE and
  // handl it with few memcopy calls
  DeSerialize(numElements, pContext);
  S(*messageArrayFixedSize)[numElements] = (S(*)[numElements])messageArray;
  DeSerialize(messageArrayFixedSize, pContext);
}
template <typename S>
void GetSerializeableLength(S* messageArray, uint64_t& numElements, uint64_t& messageSize)
{
  // TODO: HERE, if ((std::is_pod<S>::value == true) && (sizeof(S) +
  // sizeof(uint32_t) < DYNA_SERIALIZATION_FRAGMENT_SIZE)) , serialize stuff in
  // chunks with size slightly less than DYNA_SERIALIZATION_FRAGMENT_SIZE and
  // handl it with few memcopy calls
  messageSize += sizeof(uint64_t) + numElements * sizeof(S);
}
template <typename S>
void SerializeBuffer(S* messageArray, uint64_t& numElements, char*& pBuffer, uint64_t& messageSize)
{
  if (std::is_pod<S>::value == true)
  {
    SerializeBuffer(numElements, pBuffer, messageSize);
    std::memcpy(pBuffer, messageArray, sizeof(S) * numElements);
    pBuffer += sizeof(S) * numElements;
    messageSize += sizeof(S) * numElements;
  }
  else
  {
    SerializeBuffer(numElements, pBuffer, messageSize);
    for (uint64_t i = 0; i < numElements; ++i) SerializeBuffer(messageArray[i], pBuffer, messageSize);
  }
}
template <typename S>
void DeSerializeBuffer(S* messageArray, uint64_t& numElements, char*& pBuffer)
{
  if (std::is_pod<S>::value == true)
  {
    // let users clear their own structures
    // delete[] messageArray;
    numElements = 0;
    DeSerializeBuffer(numElements, pBuffer);
    messageArray = new S[numElements];
    std::memcpy(messageArray, pBuffer, sizeof(S) * numElements);
    pBuffer += sizeof(S) * numElements;
  }
  else
  {
    // let users clear their own structures
    // delete[] messageArray;
    numElements = 0;
    DeSerializeBuffer(numElements, pBuffer);
    messageArray = new S[numElements];
    for (uint64_t i = 0; i < numElements; ++i) DeSerializeBuffer(messageArray[i], pBuffer);
  }
}
// std::pair
template <typename SK, typename SV>
void Serialize(std::pair<SK, SV>& message, SerializationContext* pContext)
{
  Serialize(message.first, pContext);
  Serialize(message.second, pContext);
}
template <typename SK, typename SV>
void DeSerialize(std::pair<SK, SV>& message, SerializationContext* pContext)
{
  DeSerialize(message.first, pContext);
  DeSerialize(message.second, pContext);
}
template <typename SK, typename SV>
void GetSerializeableLength(std::pair<SK, SV>& message, uint64_t& messageSize)
{
  GetSerializeableLength(message.first, messageSize);
  GetSerializeableLength(message.second, messageSize);
}
template <typename SK, typename SV>
void SerializeBuffer(std::pair<SK, SV>& message, char*& pBuffer, uint64_t& messageSize)
{
  SerializeBuffer(message.first, pBuffer, messageSize);
  SerializeBuffer(message.second, pBuffer, messageSize);
}
template <typename SK, typename SV>
void DeSerializeBuffer(std::pair<SK, SV>& message, char*& pBuffer)
{
  DeSerializeBuffer(message.first, pBuffer);
  DeSerializeBuffer(message.second, pBuffer);
}
// std::list
template <typename S>
void Serialize(std::list<S>& messageList, SerializationContext* pContext)
{
  uint64_t listSize = messageList.size();
  Serialize(listSize, pContext);
  for (typename std::list<S>::iterator it = messageList.begin(); it != messageList.end(); ++it) Serialize(*it, pContext);
}
template <typename S>
void DeSerialize(std::list<S>& messageList, SerializationContext* pContext)
{
  uint64_t listSize = 0;
  DeSerialize(listSize, pContext);
  for (uint64_t i = 0; i < listSize; ++i)
  {
    S tmpS;
    DeSerialize(tmpS, pContext);
    messageList.push_back(std::move(tmpS));
  }
}
template <typename S>
void GetSerializeableLength(std::list<S>& messageList, uint64_t& messageSize)
{
  messageSize += sizeof(uint64_t);
  for (typename std::list<S>::iterator it = messageList.begin(); it != messageList.end(); ++it) GetSerializeableLength(*it, messageSize);
}
template <typename S>
void SerializeBuffer(std::list<S>& messageList, char*& pBuffer, uint64_t& messageSize)
{
  uint64_t listSize = 0;
  char* listSizePos = pBuffer;
  pBuffer += sizeof(uint64_t);
  for (typename std::list<S>::iterator it = messageList.begin(); it != messageList.end(); ++it)
  {
    SerializeBuffer(*it, pBuffer, messageSize);
    ++listSize;
  }
  SerializeBuffer(listSize, listSizePos, messageSize);
}
template <typename S>
void DeSerializeBuffer(std::list<S>& messageList, char*& pBuffer)
{
  // let users clear their own structures
  // messageList.clear();
  uint64_t listSize = 0;
  DeSerializeBuffer(listSize, pBuffer);
  for (uint64_t i = 0; i < listSize; ++i)
  {
    S tmpS;
    DeSerializeBuffer(tmpS, pBuffer);
    messageList.push_back(std::move(tmpS));
  }
}
// std::deque
// TODO: optimize around deque chunk size
template <typename S>
void Serialize(std::deque<S>& messageDeque, SerializationContext* pContext)
{
  uint64_t listSize = messageDeque.size();
  Serialize(listSize, pContext);
  for (typename std::deque<S>::iterator it = messageDeque.begin(); it != messageDeque.end(); ++it) Serialize(*it, pContext);
}
template <typename S>
void DeSerialize(std::deque<S>& messageDeque, SerializationContext* pContext)
{
  uint64_t listSize = 0;
  DeSerialize(listSize, pContext);
  for (uint64_t i = 0; i < listSize; ++i)
  {
    S tmpS;
    DeSerialize(tmpS, pContext);
    messageDeque.push_back(std::move(tmpS));
  }
}
template <typename S>
void GetSerializeableLength(std::deque<S>& messageDeque, uint64_t& messageSize)
{
  messageSize += sizeof(uint64_t);
  for (typename std::deque<S>::iterator it = messageDeque.begin(); it != messageDeque.end(); ++it) GetSerializeableLength(*it, messageSize);
}
template <typename S>
void SerializeBuffer(std::deque<S>& messageDeque, char*& pBuffer, uint64_t& messageSize)
{
  uint64_t listSize = 0;
  char* listSizePos = pBuffer;
  pBuffer += sizeof(uint64_t);
  for (typename std::deque<S>::iterator it = messageDeque.begin(); it != messageDeque.end(); ++it)
  {
    SerializeBuffer(*it, pBuffer, messageSize);
    ++listSize;
  }
  SerializeBuffer(listSize, listSizePos, messageSize);
}
template <typename S>
void DeSerializeBuffer(std::deque<S>& messageDeque, char*& pBuffer)
{
  // let users clear their own structures
  // messageDeque.clear();
  uint64_t listSize = 0;
  DeSerializeBuffer(listSize, pBuffer);
  for (uint64_t i = 0; i < listSize; ++i)
  {
    S tmpS;
    DeSerializeBuffer(tmpS, pBuffer);
    messageDeque.push_back(std::move(tmpS));
  }
}
// std::map
// TODO: map is sorted, store/load in-order
template <typename SK, typename SV>
void Serialize(std::map<SK, SV>& messageMap, SerializationContext* pContext)
{
  uint64_t mapSize = messageMap.size();
  Serialize(mapSize, pContext);
  for (typename std::map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    Serialize(const_cast<SK&>(it->first), pContext);
    Serialize(it->second, pContext);
  }
}
template <typename SK, typename SV>
void DeSerialize(std::map<SK, SV>& messageMap, SerializationContext* pContext)
{
  uint64_t mapSize = 0;
  DeSerialize(mapSize, pContext);
  SK tmpSK;
  SV tmpSV;
  for (uint64_t i = 0; i < mapSize; ++i)
  {
    DeSerialize(tmpSK, pContext);
    DeSerialize(tmpSV, pContext);
    messageMap.emplace(std::move(tmpSK), std::move(tmpSV));
  }
}
template <typename SK, typename SV>
void GetSerializeableLength(std::map<SK, SV>& messageMap, uint64_t& messageSize)
{
  messageSize += sizeof(uint64_t);
  for (typename std::map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    GetSerializeableLength(const_cast<SK&>(it->first), messageSize);
    GetSerializeableLength(it->second, messageSize);
  }
}
template <typename SK, typename SV>
void SerializeBuffer(std::map<SK, SV>& messageMap, char*& pBuffer, uint64_t& messageSize)
{
  uint64_t mapSize = 0;
  char* mapSizePos = pBuffer;
  pBuffer += sizeof(uint64_t);
  for (typename std::map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    SerializeBuffer(const_cast<SK&>(it->first), pBuffer, messageSize);
    SerializeBuffer(it->second, pBuffer, messageSize);
    ++mapSize;
  }
  SerializeBuffer(mapSize, mapSizePos, messageSize);
}
template <typename SK, typename SV>
void DeSerializeBuffer(std::map<SK, SV>& messageMap, char*& pBuffer)
{
  // let users clear their own structures
  // messageMap.clear();
  uint64_t mapSize = 0;
  DeSerializeBuffer(mapSize, pBuffer);
  for (uint64_t i = 0; i < mapSize; ++i)
  {
    // TODO: thread local static
    SK tmpSK;
    SV tmpSV;
    DeSerializeBuffer(tmpSK, pBuffer);
    DeSerializeBuffer(tmpSV, pBuffer);
    messageMap.insert(std::pair<SK, SV>(std::move(tmpSK), std::move(tmpSV)));
  }
}
// std::unordered_map
template <typename SK, typename SV>
void Serialize(std::unordered_map<SK, SV>& messageMap, SerializationContext* pContext)
{
  uint64_t mapSize = messageMap.size();
  Serialize(mapSize, pContext);
  for (typename std::unordered_map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    Serialize(const_cast<SK&>(it->first), pContext);
    Serialize(it->second, pContext);
  }
}
template <typename SK, typename SV>
void DeSerialize(std::unordered_map<SK, SV>& messageMap, SerializationContext* pContext)
{
  uint64_t mapSize = 0;
  DeSerialize(mapSize, pContext);
  for (uint64_t i = 0; i < mapSize; ++i)
  {
    SK tmpSK;
    SV tmpSV;
    DeSerialize(tmpSK, pContext);
    DeSerialize(tmpSV, pContext);
    messageMap.emplace(std::move(tmpSK), std::move(tmpSV));
  }
}
template <typename SK, typename SV>
void GetSerializeableLength(std::unordered_map<SK, SV>& messageMap, uint64_t& messageSize)
{
  messageSize += sizeof(uint64_t);
  for (typename std::unordered_map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    GetSerializeableLength(const_cast<SK&>(it->first), messageSize);
    GetSerializeableLength(it->second, messageSize);
  }
}
template <typename SK, typename SV>
void SerializeBuffer(std::unordered_map<SK, SV>& messageMap, char*& pBuffer, uint64_t& messageSize)
{
  uint64_t mapSize = 0;
  char* mapSizePos = pBuffer;
  pBuffer += sizeof(uint64_t);
  for (typename std::unordered_map<SK, SV>::iterator it = messageMap.begin(); it != messageMap.end(); ++it)
  {
    SerializeBuffer(const_cast<SK&>(it->first), pBuffer, messageSize);
    SerializeBuffer(it->second, pBuffer, messageSize);
    ++mapSize;
  }
  SerializeBuffer(mapSize, mapSizePos, messageSize);
}
template <typename SK, typename SV>
void DeSerializeBuffer(std::unordered_map<SK, SV>& messageMap, char*& pBuffer)
{
  // let users clear their own structures
  // messageMap.clear();
  uint64_t mapSize = 0;
  DeSerializeBuffer(mapSize, pBuffer);
  for (uint64_t i = 0; i < mapSize; ++i)
  {
    // TODO: thread local static
    SK tmpSK;
    SV tmpSV;
    DeSerializeBuffer(tmpSK, pBuffer);
    DeSerializeBuffer(tmpSV, pBuffer);
    messageMap.emplace(std::move(tmpSK), std::move(tmpSV));
  }
}
/*
// DEBUG START
struct A
{
    int a;
    int b;
    size_t c;
};
struct B
{
    char a[1000];
};
struct C
{
    SERIALIZATION_FRIENDS(C);
    virtual void foo(B & b) {b.a[0] = 0;}; // NON POD
    std::vector<B> vectorB;
    std::list<A> listA;
    std::map<std::vector<B>, std::list<A> > mapVectorBListA;
    std::vector<int> moarInts;
};
template<> void Serialize<C>(C & message, SerializationContext* pContext)
{
    Serialize(message.vectorB, pContext);
    Serialize(message.listA, pContext);
    Serialize(message.mapVectorBListA, pContext);
    Serialize(message.moarInts, pContext);
}
template<> void DeSerialize<C>(C & message, SerializationContext* pContext)
{
    DeSerialize(message.vectorB, pContext);
    DeSerialize(message.listA, pContext);
    DeSerialize(message.mapVectorBListA, pContext);
    DeSerialize(message.moarInts, pContext);
}
template<> void SerializeBuffer<C>(C & message, char * & pBuffer, uint64_t &
messageSize)
{
    SerializeBuffer(message.vectorB, pBuffer, messageSize);
    SerializeBuffer(message.listA, pBuffer, messageSize);
    SerializeBuffer(message.mapVectorBListA, pBuffer, messageSize);
    SerializeBuffer(message.moarInts, pBuffer, messageSize);
}
template<> void DeSerializeBuffer<C>(C & message, char * & pBuffer)
{
    DeSerializeBuffer(message.vectorB, pBuffer);
    DeSerializeBuffer(message.listA, pBuffer);
    DeSerializeBuffer(message.mapVectorBListA, pBuffer);
    DeSerializeBuffer(message.moarInts, pBuffer);
}
template<> void GetSerializeableLength<C>(C & message, uint64_t & messageSize)
{
    GetSerializeableLength(message.vectorB, messageSize);
    GetSerializeableLength(message.listA, messageSize);
    GetSerializeableLength(message.mapVectorBListA, messageSize);
    GetSerializeableLength(message.moarInts, messageSize);
}
bool operator==(const C& lhs, const C& rhs)
{
    return lhs.vectorB == rhs.vectorB && lhs.moarInts == rhs.moarInts &&
lhs.listA == rhs.listA && lhs.mapVectorBListA == rhs.mapVectorBListA;
}
bool operator!=(const C& lhs, const C& rhs)
{
    return !(lhs == rhs);
}
bool operator==(const B& lhs, const B& rhs)
{
    return !std::memcmp(lhs.a, rhs.a, sizeof(lhs.a));
}
bool operator!=(const B& lhs, const B& rhs)
{
    return !(lhs == rhs);
}
bool operator==(const A& lhs, const A& rhs)
{
    return lhs.a == rhs.a && lhs.b == rhs.b && lhs.c == rhs.c;
}
bool operator!=(const A& lhs, const A& rhs)
{
    return !(lhs == rhs);
}
bool operator<(const B &lhs, const B &rhs) {
  for (size_t i = 0; i < sizeof(B::a); ++i)
    if (lhs.a[i] < rhs.a[i])
      return true;
  return false;
}
void test()
{
    A a;
    B b;
    std::memset(b.a, 0, sizeof(b.a));
    C c;
    uint64_t elementSize = 0;
    SerializationContext * pSerializationContextOut = new
SerializationContext();
    {
        a.a = 1;
        a.b = 2;
        a.c = 3;
        std::strcpy(b.a, "wtfwtf1");
        c.vectorB.push_back(b);
        c.vectorB.push_back(b);
        c.listA.push_back(a);
        c.mapVectorBListA[c.vectorB] = c.listA;
        for (int i = 0; i < 16384;++i)
            c.moarInts.push_back(i%97);
        GetSerializeableLength(a, elementSize);
        GetSerializeableLength(c, elementSize);
        elementSize = 0;
        Serialize(c, pSerializationContextOut);
        Serialize(a, pSerializationContextOut);
        pSerializationContextOut->CompleteContext();
    }
    // SIMULATE NETWORK BEGIN
    SerializationContext * pSerializationContextIn = new SerializationContext();
    pSerializationContextIn->buffers = pSerializationContextOut->buffers;
    std::reverse(pSerializationContextIn->buffers.begin(),
pSerializationContextIn->buffers.end()); delete pSerializationContextOut;
    // SIMULATE NETWORK END
    C c1;
    A a1;
    {
        DeSerialize(c1, pSerializationContextIn);
        DeSerialize(a1, pSerializationContextIn);
        delete pSerializationContextIn;
    }
    if (c != c1)
        abort();
    if (a != a1)
        abort();
}
*/
